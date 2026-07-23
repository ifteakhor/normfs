use std::os::raw::c_int;

pub const MAX_VARINT32_LEN: usize = 5;
pub const MAX_VARINT64_LEN: usize = 10;

const NORMFS_UINTN_VARINT_OK: c_int = 0;
const NORMFS_UINTN_VARINT_ERR_TRUNCATED: c_int = 1;
const NORMFS_UINTN_VARINT_ERR_OVERFLOW: c_int = 2;
const NORMFS_UINTN_VARINT_ERR_NON_CANONICAL: c_int = 3;
const NORMFS_UINTN_VARINT_ERR_NO_SPACE: c_int = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarintError {
    Truncated,
    Overflow,
    NonCanonical,
    NoSpace,
    UnknownStatus(c_int),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeResult<T> {
    pub value: T,
    pub consumed: usize,
}

#[repr(C)]
struct CEncodeResult {
    written: usize,
    status: c_int,
}

#[repr(C)]
struct CDecode32Result {
    value: u32,
    consumed: usize,
    status: c_int,
}

#[repr(C)]
struct CDecode64Result {
    value: u64,
    consumed: usize,
    status: c_int,
}

unsafe extern "C" {
    fn normfs_uintn_varint32_size_ffi(value: u32) -> usize;
    fn normfs_uintn_varint64_size_ffi(value: u64) -> usize;

    fn normfs_uintn_varint32_encode_ffi(value: u32, out: *mut u8, out_len: usize) -> CEncodeResult;
    fn normfs_uintn_varint64_encode_ffi(value: u64, out: *mut u8, out_len: usize) -> CEncodeResult;

    fn normfs_uintn_varint32_decode_ffi(buf: *const u8, len: usize) -> CDecode32Result;
    fn normfs_uintn_varint64_decode_ffi(buf: *const u8, len: usize) -> CDecode64Result;
}

#[inline]
fn map_status(status: c_int) -> Result<(), VarintError> {
    match status {
        NORMFS_UINTN_VARINT_OK => Ok(()),
        NORMFS_UINTN_VARINT_ERR_TRUNCATED => Err(VarintError::Truncated),
        NORMFS_UINTN_VARINT_ERR_OVERFLOW => Err(VarintError::Overflow),
        NORMFS_UINTN_VARINT_ERR_NON_CANONICAL => Err(VarintError::NonCanonical),
        NORMFS_UINTN_VARINT_ERR_NO_SPACE => Err(VarintError::NoSpace),
        other => Err(VarintError::UnknownStatus(other)),
    }
}

#[inline]
pub fn encoded_len_u32(value: u32) -> usize {
    unsafe { normfs_uintn_varint32_size_ffi(value) }
}

#[inline]
pub fn encoded_len_u64(value: u64) -> usize {
    unsafe { normfs_uintn_varint64_size_ffi(value) }
}

#[inline]
pub fn encode_u32(value: u32, out: &mut [u8]) -> Result<usize, VarintError> {
    let result = unsafe { normfs_uintn_varint32_encode_ffi(value, out.as_mut_ptr(), out.len()) };
    map_status(result.status)?;
    Ok(result.written)
}

#[inline]
pub fn encode_u64(value: u64, out: &mut [u8]) -> Result<usize, VarintError> {
    let result = unsafe { normfs_uintn_varint64_encode_ffi(value, out.as_mut_ptr(), out.len()) };
    map_status(result.status)?;
    Ok(result.written)
}

#[inline]
pub fn encode_u32_array(value: u32) -> ([u8; MAX_VARINT32_LEN], usize) {
    let mut out = [0u8; MAX_VARINT32_LEN];
    let written = encode_u32(value, &mut out).expect("fixed varint32 buffer is large enough");
    (out, written)
}

#[inline]
pub fn encode_u64_array(value: u64) -> ([u8; MAX_VARINT64_LEN], usize) {
    let mut out = [0u8; MAX_VARINT64_LEN];
    let written = encode_u64(value, &mut out).expect("fixed varint64 buffer is large enough");
    (out, written)
}

#[inline]
pub fn decode_u32(input: &[u8]) -> Result<DecodeResult<u32>, VarintError> {
    let result = unsafe { normfs_uintn_varint32_decode_ffi(input.as_ptr(), input.len()) };
    map_status(result.status)?;
    Ok(DecodeResult {
        value: result.value,
        consumed: result.consumed,
    })
}

#[inline]
pub fn decode_u64(input: &[u8]) -> Result<DecodeResult<u64>, VarintError> {
    let result = unsafe { normfs_uintn_varint64_decode_ffi(input.as_ptr(), input.len()) };
    map_status(result.status)?;
    Ok(DecodeResult {
        value: result.value,
        consumed: result.consumed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_u32(value: u32, expected_len: usize) {
        assert_eq!(encoded_len_u32(value), expected_len);
        let (buf, written) = encode_u32_array(value);
        assert_eq!(written, expected_len);
        let decoded = decode_u32(&buf[..written]).unwrap();
        assert_eq!(decoded.value, value);
        assert_eq!(decoded.consumed, expected_len);
    }

    fn roundtrip_u64(value: u64, expected_len: usize) {
        assert_eq!(encoded_len_u64(value), expected_len);
        let (buf, written) = encode_u64_array(value);
        assert_eq!(written, expected_len);
        let decoded = decode_u64(&buf[..written]).unwrap();
        assert_eq!(decoded.value, value);
        assert_eq!(decoded.consumed, expected_len);
    }

    #[test]
    fn varint32_roundtrips_boundary_values() {
        roundtrip_u32(0, 1);
        roundtrip_u32(127, 1);
        roundtrip_u32(128, 2);
        roundtrip_u32(16_383, 2);
        roundtrip_u32(16_384, 3);
        roundtrip_u32(0x1f_ffff, 3);
        roundtrip_u32(0x20_0000, 4);
        roundtrip_u32(0x0fff_ffff, 4);
        roundtrip_u32(0x1000_0000, 5);
        roundtrip_u32(u32::MAX, 5);
    }

    #[test]
    fn varint64_roundtrips_boundary_values() {
        roundtrip_u64(0, 1);
        roundtrip_u64(127, 1);
        roundtrip_u64(128, 2);
        roundtrip_u64(0x3fff, 2);
        roundtrip_u64(0x4000, 3);
        roundtrip_u64(0x7fff_ffff_ffff_ffff, 9);
        roundtrip_u64(u64::MAX, 10);
    }

    #[test]
    fn decode_rejects_truncated_inputs() {
        assert_eq!(decode_u32(&[0x80]), Err(VarintError::Truncated));
        assert_eq!(decode_u64(&[0x80]), Err(VarintError::Truncated));
    }

    #[test]
    fn decode_rejects_non_canonical_inputs() {
        assert_eq!(decode_u32(&[0x80, 0x00]), Err(VarintError::NonCanonical));
        assert_eq!(decode_u64(&[0x80, 0x00]), Err(VarintError::NonCanonical));
    }

    #[test]
    fn decode_rejects_overflow_inputs() {
        assert_eq!(
            decode_u32(&[0xff, 0xff, 0xff, 0xff, 0x10]),
            Err(VarintError::Overflow)
        );

        assert_eq!(
            decode_u64(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]),
            Err(VarintError::Overflow)
        );
    }

    #[test]
    fn encode_reports_short_output_buffer() {
        let mut out = [0u8; 1];
        assert_eq!(encode_u32(128, &mut out), Err(VarintError::NoSpace));
        assert_eq!(encode_u64(128, &mut out), Err(VarintError::NoSpace));
    }
}
