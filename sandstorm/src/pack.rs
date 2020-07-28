/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use std::mem;
use std::slice;

/// Indicates a type is safe for the database to cast between raw bytes and values. Only types
/// endorsed by this trait will be accepted by the unpack and consume functions in this module.
pub unsafe trait Safe {}

/// Full list of types the database is willing to zero-copy unpack by casting. These types can also
/// be composed see the various versions of unpack and consume.
unsafe impl Safe for usize {}
unsafe impl Safe for isize {}
unsafe impl Safe for u64 {}
unsafe impl Safe for i64 {}
unsafe impl Safe for u32 {}
unsafe impl Safe for i32 {}
unsafe impl Safe for u16 {}
unsafe impl Safe for i16 {}
unsafe impl Safe for u8 {}
unsafe impl Safe for i8 {}
unsafe impl Safe for f64 {} // TODO(stutsman) Are floats total or are there unsafe values?
unsafe impl Safe for f32 {}
unsafe impl Safe for bool {}
unsafe impl Safe for () {}

/// Creates a `&'a A` that treats the bytes in `args` as an `A` without copying them. Also, returns a
/// slice that has been advanced by the number of bytes that `A` occupies to make parsing
/// slices with many records easier.
///
/// # Arguments
/// * `args`: Bytes that will underlie the returned reference of type `&'a A`.
///    If the start of this slice isn't aligned with `A`'s alignment requirements or if
///    this slice is smaller than `A`'s size `None` is returned.
/// # Return
/// None if `args` isn't aligned according to `A`'s alignment requirement or if the bytes covered
/// by `args` is less than the size of `A`. Otherwise, an `&'a A' is returned and a new slice that
/// identical to `args` but with the start advanced to remove the bytes of the returned `A` from
/// view. `A` must be one of the `Safe` types listed above.
pub fn consume<'a, A>(args: &'a [u8]) -> Option<(&'a A, &'a [u8])>
where
    A: Safe,
{
    Some((cast(args).unwrap(), &args[mem::size_of::<A>()..]))
}

/// See `consume`. Identical except it returns a reference to a two-tuple comprised of the `Safe`
/// types listed above.
pub fn consume_two<'a, A, B>(args: &'a [u8]) -> Option<(&'a (A, B), &'a [u8])>
where
    A: Safe,
    B: Safe,
{
    Some((cast(args).unwrap(), &args[mem::size_of::<(A, B)>()..]))
}

/// See `consume_two`.
pub fn consume_three<'a, A, B, C>(args: &'a [u8]) -> Option<(&'a (A, B, C), &'a [u8])>
where
    A: Safe,
    B: Safe,
    C: Safe,
{
    Some((cast(args).unwrap(), &args[mem::size_of::<(A, B, C)>()..]))
}

/// See `consume_two`.
pub fn consume_four<'a, A, B, C, D>(args: &'a [u8]) -> Option<(&'a (A, B, C, D), &'a [u8])>
where
    A: Safe,
    B: Safe,
    C: Safe,
    D: Safe,
{
    Some((cast(args).unwrap(), &args[mem::size_of::<(A, B, C, D)>()..]))
}

/// Creates a `&'a A` that treats the bytes in `args` as an `A` without copying them.
///
/// # Arguments
/// * `args`: Bytes that will underlie the returned reference of type `&'a A`.
///    If the start of this slice isn't aligned with `A`'s alignment requirements or if
///    this slice is smaller than `A`'s size `None` is returned.
/// # Return
/// None if `args` isn't aligned according to `A`'s alignment requirement or if the bytes covered
/// by `args` is less than the size of `A`. Otherwise, an `&'a A' is returned.
/// `A` must be one of the `Safe` types listed above.
pub fn unpack<'a, A>(args: &'a [u8]) -> Option<&'a A>
where
    A: Safe,
{
    cast(args)
}

/// See `unpack`. Identical except it returns a reference to a one-tuple comprised of the `Safe`
/// types listed above.
pub fn unpack_one<'a, A>(args: &'a [u8]) -> Option<&'a (A,)>
where
    A: Safe,
{
    cast(args)
}

/// See `unpack_one`.
pub fn unpack_two<'a, A, B>(args: &'a [u8]) -> Option<&'a (A, B)>
where
    A: Safe,
    B: Safe,
{
    cast(args)
}

/// See `unpack_one`.
pub fn unpack_three<'a, A, B, C>(args: &'a [u8]) -> Option<&'a (A, B, C)>
where
    A: Safe,
    B: Safe,
    C: Safe,
{
    cast(args)
}

/// See `unpack_one`.
pub fn unpack_four<'a, A, B, C, D>(args: &'a [u8]) -> Option<&'a (A, B, C, D)>
where
    A: Safe,
    B: Safe,
    C: Safe,
    D: Safe,
{
    cast(args)
}

/// Creates a byte slice from a reference to a safe type without *any* intermediate copies.
///
/// # Arguments
///
/// * `arg`: The argument to be "packed up" into a byte slice.
///
/// # Return
/// A byte slice corresponding to the passed in argument with the same alignment.
pub fn pack<'a, A>(arg: &'a A) -> &'a [u8]
where
    A: Safe,
{
    let p = (arg as *const A) as *const u8;
    let l = mem::size_of::<A>();
    unsafe { slice::from_raw_parts(p, l) }
}

/// Creates a `&'a A` that treats the bytes in `args` as an `A` without copying them.
///
/// # Arguments
/// * `args`: Bytes that will underlie the returned reference of type `&'a A`.
///    If the start of this slice isn't aligned with `A`'s alignment requirements or if
///    this slice is smaller than `A`'s size `None` is returned.
/// # Return
/// None if `args` isn't aligned according to `A`'s alignment requirement or if the bytes covered
/// by `args` is less than the size of `A`. Otherwise, an `&'a A' is returned.
/// `A` must be one of the `Safe` types listed above.
fn cast<'a, T>(args: &'a [u8]) -> Option<&'a T> {
    if mem::size_of::<T>() <= args.len() {
        let p = args.as_ptr();
        if (p as usize & (mem::align_of::<T>() - 1)) == 0 {
            unsafe { Some(&*(args.as_ptr() as *const T)) }
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_unpack() {
        let mut args = [0u8; 16];
        for i in 0..16 {
            args[i] = i as u8;
        }
        let value: &u64 = unpack(&args[0..8]).unwrap();
        assert_eq!(0x0706050403020100u64, *value);

        let value: &(u64,) = unpack_one(&args[0..8]).unwrap();
        assert_eq!(0x0706050403020100u64, value.0);

        // Violate alignment of 64-bit single field struct.
        let value: Option<&(u64,)> = unpack_one(&args[1..9]);
        assert_eq!(None, value);

        assert_eq!(16, mem::size_of::<(u64, u8, u32)>());
        let value: &(u64, u8, u32) = unpack_three(&args[0..16]).unwrap();
        assert_eq!(0x0706050403020100u64, value.0);
        assert_eq!(0x08u8, value.1);
        assert_eq!(0x0f0e0d0cu32, value.2);

        // Mismatched struct size versus buffer size.
        let value: Option<&(u64,)> = unpack_one(&args[0..7]);
        assert_eq!(None, value);
    }

    type OType = u16;
    type ObjectId = u32;
    type Assoc = (ObjectId, ObjectId, OType);

    fn src(assoc: &Assoc) -> ObjectId {
        assoc.0
    }
    fn dst(assoc: &Assoc) -> ObjectId {
        assoc.1
    }
    fn otype(assoc: &Assoc) -> OType {
        assoc.2
    }

    #[test]
    fn test_unpack_sugar() {
        assert_eq!(12, mem::size_of::<Assoc>());
        assert_eq!(4, mem::align_of::<Assoc>());

        let args: Vec<u8> = vec![
            0x03, 0x00, 0x00, 0x00, // assoc_count
            0x01, 0x00, 0x00, 0x00, // assoc[0].src
            0x02, 0x00, 0x00, 0x00, // assoc[0].dst
            0x01, 0x01, 0x00, 0x00, // assoc[0].otype + 2 bytes pad
            0x03, 0x00, 0x00, 0x00, // assoc[1].src
            0x04, 0x00, 0x00, 0x00, // assoc[1].dst
            0x02, 0x02, 0x00, 0x00, // assoc[1].otype + 2 bytes pad
            0x05, 0x00, 0x00, 0x00, // assoc[2].src
            0x06, 0x00, 0x00, 0x00, // assoc[2].dst
            0x03, 0x03, 0x00, 0x00, // assoc[2].otype + 2 bytes pad
        ];
        let args = args.as_slice();

        let (assoc_count, args): (&u32, _) = consume(args).unwrap();
        assert_eq!(3, *assoc_count);

        let (assoc, args): (&Assoc, _) = consume_three(args).unwrap();
        assert_eq!(1, src(assoc));
        assert_eq!(2, dst(assoc));
        assert_eq!(0x0101u16, otype(assoc));

        let (assoc, args): (&Assoc, _) = consume_three(args).unwrap();
        assert_eq!(3, src(assoc));
        assert_eq!(4, dst(assoc));
        assert_eq!(0x0202u16, otype(assoc));

        let (assoc, _): (&Assoc, _) = consume_three(args).unwrap();
        assert_eq!(5, src(assoc));
        assert_eq!(6, dst(assoc));
        assert_eq!(0x0303u16, otype(assoc));
    }
}
