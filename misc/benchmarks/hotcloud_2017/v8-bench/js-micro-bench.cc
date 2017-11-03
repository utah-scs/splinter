// Copyright 2015 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <chrono>
#include <iostream>

#include "include/libplatform/libplatform.h"
#include "include/v8.h"

using namespace std;
using namespace v8;

void Null(const v8::FunctionCallbackInfo<v8::Value>& args) {}

const char* ToCString(const v8::String::Utf8Value& value) {
  return *value ? *value : "<string conversion failed>";
}

void Print(const v8::FunctionCallbackInfo<v8::Value>& args) {
  bool first = true;
  for (int i = 0; i < args.Length(); i++) {
    v8::HandleScope handle_scope(args.GetIsolate());
    if (first) {
      first = false;
    } else {
      printf(" ");
    }
    v8::String::Utf8Value str(args[i]);
    const char* cstr = ToCString(str);
    printf("%s", cstr);
  }
  printf("\n");
  fflush(stdout);
}

v8::MaybeLocal<v8::String> ReadFile(const char* name);

v8::MaybeLocal<String> ReadFile(Isolate* isolate, const string& name) {
    FILE* file = fopen(name.c_str(), "rb");
    if (file == NULL) return MaybeLocal<String>();

    fseek(file, 0, SEEK_END);
    size_t size = ftell(file);
    rewind(file);

    char* chars = new char[size + 1];
    chars[size] = '\0';
    for (size_t i = 0; i < size;) {
        i += fread(&chars[i], 1, size - i, file);
        if (ferror(file)) {
            fclose(file);
            return MaybeLocal<String>();
        }
    }
    fclose(file);
    MaybeLocal<String> result = String::NewFromUtf8(isolate, 
                                                    chars, 
                                                    NewStringType::kNormal, 
                                                    static_cast<int>(size));
    delete[] chars;
    return result;
}

int main(int argc, char* argv[]) {
  typedef std::chrono::high_resolution_clock Time;
  typedef std::chrono::milliseconds ms;
  typedef std::chrono::microseconds us;
  typedef std::chrono::duration<float> fsec;
  // Initialize V8.
  V8::InitializeICUDefaultLocation(argv[0]);
  V8::InitializeExternalStartupData(argv[0]);
  Platform* platform = platform::CreateDefaultPlatform();
  V8::InitializePlatform(platform);
  V8::Initialize();

  // Create a new Isolate and make it the current one.
  Isolate::CreateParams create_params;
  create_params.array_buffer_allocator = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
  Isolate* isolate = Isolate::New(create_params);
  {
    Isolate::Scope isolate_scope(isolate);

    // Create a stack-allocated handle scope.
    HandleScope handle_scope(isolate);

    v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate);
    global->Set(v8::String::NewFromUtf8(
                  isolate, "null_func", v8::NewStringType::kNormal).ToLocalChecked(),
              v8::FunctionTemplate::New(isolate, Null));

    global->Set(
      v8::String::NewFromUtf8(isolate, "print", v8::NewStringType::kNormal)
          .ToLocalChecked(),
      v8::FunctionTemplate::New(isolate, Print));

    auto before = Time::now();

    // Create a new context.
    Local<Context> context = Context::New(isolate, NULL, global);

    auto after = Time::now();
    fsec gap = after - before;
    us u = std::chrono::duration_cast<us>(gap);
    cout << "first time create context costs " << u.count() << " " << "us" << endl;

    before = Time::now();

    // Create a new context.
    Local<Context> context2 = Context::New(isolate);

    after = Time::now();
    gap = after - before;
    u = std::chrono::duration_cast<us>(gap);
    cout << "second time create context costs " << u.count() << " " << "us" << endl;

    before = Time::now();
/*    for (int i = 0; i < 100; i++) { 
     
      // Create a new context.
      Local<Context> c = Context::New(isolate);
     
    }*/
    after = Time::now();
    gap = after - before;
    u = std::chrono::duration_cast<us>(gap);
    cout << "average create context costs " << u.count()/100 << " " << "us" << endl;

    // Enter the context for compiling and running the hello world script.
    Context::Scope context_scope(context);

    // Create a string containing the JavaScript source code.
    //Local<String> source =
      //  String::NewFromUtf8(isolate, "'Hello' + ', World!'",
        //                    NewStringType::kNormal).ToLocalChecked();
    Local<String> source;
          if (!ReadFile(isolate, "call_cpp.js").ToLocal(&source)) {
                              fprintf(stderr, "Error reading file\n");}

    before = Time::now();
    // Compile the source code.
    Local<Script> script = Script::Compile(context, source).ToLocalChecked();

    after = Time::now();
    gap = after - before;
    u = std::chrono::duration_cast<us>(gap);
    cout << "compiling costs " << u.count() << " " << "us" << endl;

    // Run the script to get the result.
//    Local<Value> result;
    before = Time::now();

    Local<Value> result = script->Run(context).ToLocalChecked();

    after = Time::now();
    gap = after - before;
    u = std::chrono::duration_cast<us>(gap);
    cout << "run script costs " << u.count() << " " << "us" << endl;


    // Convert the result to an UTF8 string and print it.
    //String::Utf8Value utf8(result);
//    printf("%s\n", *utf8);

    Local<String> process_name = String::NewFromUtf8(isolate, 
                                                     "null_func", 
                                                     NewStringType::kNormal)
                                                     .ToLocalChecked();
    Local<Value> process_val;
    if (!context->Global()->Get(context, process_name).ToLocal(&process_val) ||
                                !process_val->IsFunction()) {
                                                               return 0;
                                                            }

    Local<Function> process_fun = Local<Function>::Cast(process_val);

    before = Time::now();
    result = process_fun->Call(context, context->Global(), 0, NULL).ToLocalChecked();
    after = Time::now();
    gap = after - before;
    u = std::chrono::duration_cast<us>(gap);
    cout << "first time function call costs " << u.count() << " " << "us" << endl;

    before = Time::now();
    for (int i = 0; i < 1000; i++) { 
      result = process_fun->Call(context, context->Global(), 0, NULL).ToLocalChecked();
    }
    after = Time::now();
    gap = after - before;
    u = std::chrono::duration_cast<us>(gap);
    cout << "subsequent function call costs " << u.count() << " " << "ns" << endl;

    UniquePersistent<Context> persist_context;
    Local<Context> new_context = Context::New(isolate);
    UniquePersistent<Script> ps;
    ps.Reset(isolate, script);
    persist_context.Reset(isolate, new_context);
    UniquePersistent<Function> f;
    f.Reset(isolate, process_fun);
    before = Time::now();
    for (int i = 0; i < 1000; i++) { 
//      Local<Context> c = Local<Context>::New(isolate, persist_context);
    /*  if (!c->Global()->Set(c,
                      v8::String::NewFromUtf8(isolate, "key", v8::NewStringType::kNormal)
                          .ToLocalChecked(),
                      v8::String::NewFromUtf8(isolate, "value",v8::NewStringType::kNormal)
                          .ToLocalChecked()).FromJust()) return 0;
*/
//      Context::Scope context_scope(c);
      //Local<Script> s =
        //              Local<Script>::New(isolate, ps);
      //s->Run(c).ToLocalChecked();
//      Local<Function> fun = Local<Function>::New(isolate, f);
      result = process_fun->Call(context, context->Global(), 0 , NULL).ToLocalChecked();
    }
    after = Time::now();
    gap = after - before;
    u = std::chrono::duration_cast<us>(gap);

    cout << "context switch cost " << u.count() << " " << "ns" << endl;
  }

  // Dispose the isolate and tear down V8.
  isolate->Dispose();
  V8::Dispose();
  V8::ShutdownPlatform();
  delete platform;
  delete create_params.array_buffer_allocator;
  return 0;
}
