#include <cinttypes>
#include <iostream>
#include <vector>
#include <atomic>
#include <cassert>
#include <string>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <string>
#include <unordered_map>

#include <sys/mman.h>

#include "Benchmark.h"

#include "include/libplatform/libplatform.h"
#include "include/v8.h"

enum class Mode { NATIVE, JS, PROCESS };
static constexpr Mode mode = Mode::MODE;

using namespace v8;

static thread_local PRNG prng{};

class Mutex {
 private:
    void *mem;
    std::atomic<int>* vals;
    size_t nBytes;

 public:
  Mutex(size_t nThreads)
    : mem{}
    , vals{}
    , nBytes{sizeof(vals[0]) * nThreads}
  {
    assert(nBytes == sizeof(int) * nThreads);
    mem = mmap(NULL,
               sizeof(vals[0]) * nThreads,
               PROT_READ | PROT_WRITE,
               MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    assert(mem != MAP_FAILED);
    bzero(mem, nBytes);

    vals = reinterpret_cast<decltype(vals)>(mem);
  }

  ~Mutex()
  {
    munmap(mem, nBytes);
  }

  void lock(size_t threadId)
  {
    auto& val = vals[threadId];
    int expected = 0;
    if (!val.compare_exchange_strong(expected, 1)) {
      while (true) {
        if (expected == 2 || !val.compare_exchange_strong(expected, 2)) {
          if (expected != 0)
            futex_wait(reinterpret_cast<int*>(&val), 2);
        }
        expected = 0;
        if (val.compare_exchange_strong(expected, 2))
          break;
       }
    }
  }

  void unlock(size_t threadId)
  {
    auto& val = vals[threadId];
    --val;
    val = 0;
    futex_wake(reinterpret_cast<int*>(&val), 1);
  }
};

class DB {
  std::unordered_map<std::string, std::string> map;

  public:
    DB(size_t nKeys)
      : map{}
    {
    }

    std::string get(const std::string& key) {
      // WARNING: Weird semantics here on not present keys.
      return map.at(key);
    }

    void put(const std::string& key, const std::string& value) {
      map.emplace(key, value);
    }

  private:
    DB(DB&&) = delete;
    DB& operator=(DB&&) = delete;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;
};

DB* globalDB = nullptr;

void DBGet(const FunctionCallbackInfo<Value>& args) {
  if (args.Length() != 1) {
    args.GetIsolate()->ThrowException(
      String::NewFromUtf8(args.GetIsolate(), "Bad parameters",
          NewStringType::kNormal).ToLocalChecked());
    return;
  }

  String::Utf8Value key{args[0]};
  std::string r = globalDB->get(*key);
  MaybeLocal<String> value =
    String::NewFromUtf8(args.GetIsolate(), r.data(), NewStringType::kNormal);
  Local<String> ret = value.ToLocalChecked();

  args.GetReturnValue().Set(ret);
}

void DBPut(const FunctionCallbackInfo<Value>& args) {
  if (args.Length() != 2) {
    args.GetIsolate()->ThrowException(
      String::NewFromUtf8(args.GetIsolate(), "Bad parameters",
          NewStringType::kNormal).ToLocalChecked());
    return;
  }

  String::Utf8Value key{args[0]};
  String::Utf8Value value{args[1]};
  globalDB->put(*key, *value);
}

v8::Local<v8::Context> CreateDBContext(v8::Isolate* isolate) {
  // Create a template for the global object.
  v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate);
  // Bind the global 'print' function to the C++ Print callback.
  global->Set(
      v8::String::NewFromUtf8(isolate, "DBGet", v8::NewStringType::kNormal)
          .ToLocalChecked(),
      v8::FunctionTemplate::New(isolate, DBGet));
  global->Set(
      v8::String::NewFromUtf8(isolate, "DBPut", v8::NewStringType::kNormal)
          .ToLocalChecked(),
      v8::FunctionTemplate::New(isolate, DBPut));

  auto key = std::to_string(0);
  global->Set(
    v8::String::NewFromUtf8(isolate, "key", v8::NewStringType::kNormal)
        .ToLocalChecked(),
    v8::String::NewFromUtf8(isolate, key.c_str(),v8::NewStringType::kNormal)
        .ToLocalChecked());


  return v8::Context::New(isolate, NULL, global);
}

typedef uint64_t UID;

class User {
  public:
  UID uid;
  std::vector<UniquePersistent<Context>> contexts;
  std::vector<UniquePersistent<Script>> scripts;

  static UID nextUID;

  User()
    : uid{nextUID++}
    , contexts{}
    , scripts{}
  {
  }

  ~User()
  {
    for (auto& script : scripts)
      script.Reset();
    for (auto& context : contexts)
      context.Reset();
  }

  std::string q1(DB& db, const std::string& key) {
    // XXX "0" since JS is always zero too; fix when JS is fixed.
    return db.get(db.get("0"));
  }

  std::string jsQ1(
      Local<Context>& context,
      Local<Script>& script,
      DB& db,
      const std::string& key)
  {
    Local<Value> result = script->Run(context).ToLocalChecked();
    String::Utf8Value utf8(result);
    return {*utf8};
  }

  User(const User& user) = delete;
  User& operator=(const User& user) = delete;
  User(User&& user) = delete;
  User& operator=(User&& user) = delete;
};
UID User::nextUID = 0;

class ContextSwitchBench : public Benchmark {
  DB db;
  std::vector<Isolate::CreateParams> params;
  std::vector<Isolate*> isolates;
  std::vector<User> users;

  const size_t valueLen;
  const size_t nKeys;

  // WARNING We'll want to make these thread_local and aggregate.
  std::atomic<uint64_t> nGets;
  std::atomic<uint64_t> nSets;

  uint64_t lastNGets;

  Mutex startMutex;
  Mutex doneMutex;

  void warmup(size_t threadId) {
    pinTo(threadId);
    prng.reseed(threadId);

    if (mode == Mode::PROCESS) {
      for (int i = 0; i < nThreads; i++) {
        pid_t pid = fork();
        if (pid == 0) {
          int r = prctl(PR_SET_PDEATHSIG, SIGHUP);
          if (r == -1) {exit(1); }
          while (1) {
            startMutex.lock(threadId);
            auto key = std::to_string(prng() % nKeys);
            //auto& user = users[prng() % users.size()];
            auto& user = users[nGets & 1];
            user.q1(db, key);
            doneMutex.unlock(threadId);
          }
        }
      }
    } else if (mode == Mode::JS) {
      params.at(threadId).array_buffer_allocator =
          v8::ArrayBuffer::Allocator::NewDefaultAllocator();
      isolates.at(threadId) = Isolate::New(params.at(threadId));
     
      for (auto& user : users) {
        v8::Isolate* isolate = isolates[threadId];
        assert(isolate);
        Isolate::Scope isolateScope{isolate};
        HandleScope handleScope{isolate};
     
        v8::Local<v8::Context> context = CreateDBContext(isolate);
        if (context.IsEmpty()) {
          fprintf(stderr, "Error creating context\n");
          exit(-1);
        }
        user.contexts.at(threadId).Reset(isolate, context);
     
        Context::Scope contextScope(context);
     
        Local<String> source =
            String::NewFromUtf8(isolate, "",
                                NewStringType::kNormal).ToLocalChecked();
        Local<Script> script =
            Script::Compile(context, source).ToLocalChecked();
        user.scripts.at(threadId).Reset(isolate, script);
      }
    }
  }

  // Just do round-robin gets until time is up.
  void run(size_t threadId) {
    while (!getStop()) {
      auto key = std::to_string(prng() % nKeys);
      auto& user = users[prng() % users.size()];

      if (mode == Mode::PROCESS) {
        startMutex.unlock(threadId);
        doneMutex.lock(threadId);
      } else if (mode == Mode::JS) {
        Isolate* isolate = isolates[threadId];
        assert(isolate);
        Isolate::Scope isolateScope{isolate};
        HandleScope handleScope{isolate};
        Local<Context> context =
          Local<Context>::New(isolate, user.contexts[threadId]);

        Context::Scope contextScope{context};

        Local<Script> script =
          Local<Script>::New(isolate, user.scripts[threadId]);

        user.jsQ1(context, script, db, key);
      } else {
        user.q1(db, key);
      }
      ++nGets;
    }
  }

  void dumpHeader() {
    std::cout << "time" << " "
              << "gets " << " "
              << "sets " << " "
              << "getsPerSec" << " "
              << "usPerQuery" << " "
              << std::endl;
  }

  void dump(double time, double interval) {
    const uint64_t intervalGets = nGets - lastNGets;
    std::cout << time << " "
              << nGets << " "
              << nSets << " "
              << intervalGets / interval << " "
              << (interval * 1e6 / intervalGets) * nThreads << " "
              << std::endl;
    lastNGets = nGets;
  }

 public:
  ContextSwitchBench(size_t nThreads, double seconds,
                     size_t valueLen, size_t nKeys,
                     size_t nUsers)
    : Benchmark{nThreads, seconds}
    , db{nKeys}
    , params{nThreads}
    , isolates{nThreads}
    , users{nThreads}
    , valueLen{valueLen}
    , nKeys{nKeys}
    , nGets{}
    , nSets{}
    , lastNGets{}
    , startMutex{nThreads}
    , doneMutex{nThreads}
  {
    // XXX Blech.
    globalDB = &db;

    for (auto& user : users) {
      user.contexts.resize(nThreads);
      user.scripts.resize(nThreads);
    }
  }

  ~ContextSwitchBench()
  {
    users.clear();

    // Dispose the isolate and tear down V8.
    for (Isolate* isolate : isolates)
      isolate->Dispose();
    for (auto& param : params)
      delete param.array_buffer_allocator;
  }

  // Do normal benchmark start stuff, but then also prefill with some data.
  // This happens on the main thread, so it happens just once in the order
  // specified.
  void start() {
    for (int i = 0; i < nThreads; i++) {
      startMutex.lock(i);
      doneMutex.lock(i);
    }

    std::vector<size_t> values{};
    for (size_t i = 0; i < nKeys; ++i)
      values.emplace_back(i);
    std::random_shuffle(values.begin(), values.end());

    // Fill with dummy data.
    for (size_t i = 0; i < nKeys; ++i)
      db.put(std::to_string(i), std::to_string(values[i]));

    Benchmark::start();
  }
};

int main(int argc, char* argv[]) {
  size_t nThreads = 1;
  double seconds = 10.0;
  size_t valueLen = 1024;
  size_t nKeys = 1024 * 1024;
  size_t nUsers = 1024;

  Platform* platform;
  if (mode == Mode::JS) {
    // Initialize V8.
    V8::InitializeICUDefaultLocation(argv[0]);
    V8::InitializeExternalStartupData(argv[0]);
    platform = platform::CreateDefaultPlatform();
    V8::InitializePlatform(platform);
    V8::Initialize();
  }

  int c;
  while ((c = getopt(argc, argv, "t:s:k:T:u:")) != -1) {
    switch (c)
    {
      case 't':
        seconds = std::stod(optarg);
        break;
      case 's':
        valueLen = std::stoul(optarg);
        break;
      case 'k':
        nKeys = std::stoul(optarg);
        assert((nKeys ^ (nKeys - 1)) == (nKeys * 2 - 1));
        break;
      case 'u':
        nUsers = std::stoul(optarg);
        break;
      case 'T':
        nThreads = std::stoul(optarg);
        break;
      default:
        std::cerr << "Unknown argument" << std::endl;
        exit(-1);
    }
  }

  ContextSwitchBench bench{nThreads, seconds, valueLen, nKeys, nUsers};
  std::cout << "# nthreads " << nThreads
            << " valuelen " << valueLen
            << " nkeys " << nKeys
            << " nusers " << nUsers
            << " seconds " << seconds
            << std::endl;

  bench.start();

  if (mode == Mode::JS) {
    V8::Dispose();
    V8::ShutdownPlatform();
    delete platform;
  }

  return 0;
}
