use bytes::BytesMut;
use std::io::Cursor;
use std::os::raw::c_int;
use tokio::io::{AsyncRead, AsyncReadExt};
use uintn::{UintN, varint};

use super::crc32c;
use super::wal_entry_v1::{WalEntryV1Error, derive_entry_id};
use super::wal_header::{WalHeader, WalHeaderError};

pub const WAL_HEADER_V0_VERSION: u64 = 0;
pub const WAL_HEADER_V1_VERSION: u64 = 1;

/// The version word keeps its V0 encoding, a fixed 8 byte little-endian u64,
/// so that readers predating V1 still report an unsupported version instead of
/// misparsing the file.
pub const WAL_HEADER_VERSION_SIZE: usize = 8;
pub const WAL_HEADER_V1_MIN_SIZE: usize = 11;
pub const WAL_HEADER_V1_MAX_SIZE: usize = 20;

/// A V1 file's entry ids are `num_entries_before + index`, so a flipped bit in
/// the header renumbers every entry in the file while each entry's own CRC
/// still verifies — the entry checksum covers `[varint][record]` and the id is
/// not stored. The trailer closes that: the fields are checksummed as a unit.
///
/// It sits outside `normfs_wal_header_v1_encode`/`_decode` on purpose. Those
/// carry the byte-level Frama-C contracts and the `decode(encode(h)) == h`
/// theorem; making the C decoder reject on CRC would put that theorem behind
/// the same frame lemma the entry codec could not discharge, so the framing
/// lives here and the proven codec stays untouched.
pub const WAL_HEADER_V1_CRC_SIZE: usize = 4;

const NORMFS_WAL_HEADER_OK: c_int = 0;
const NORMFS_WAL_HEADER_ERR_TRUNCATED: c_int = 1;
const NORMFS_WAL_HEADER_ERR_OVERFLOW: c_int = 2;
const NORMFS_WAL_HEADER_ERR_NON_CANONICAL: c_int = 3;
const NORMFS_WAL_HEADER_ERR_NO_SPACE: c_int = 4;
const NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION: c_int = 5;
const NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE: c_int = 6;
const NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE: c_int = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalHeaderV1Error {
    Truncated,
    Overflow,
    NonCanonical,
    NoSpace,
    UnsupportedVersion(u64),
    InvalidDataSizeBytes(u64),
    InvalidIdSizeBytes(u64),
    ValueTooLarge,
    UnknownStatus(c_int),
    CrcMismatch { stored: u32, computed: u32 },
}

impl std::fmt::Display for WalHeaderV1Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalHeaderV1Error::Truncated => write!(f, "Truncated V1 WAL header"),
            WalHeaderV1Error::Overflow => write!(f, "V1 WAL header varint exceeds u64"),
            WalHeaderV1Error::NonCanonical => write!(f, "Non-canonical V1 WAL header varint"),
            WalHeaderV1Error::NoSpace => write!(f, "Buffer too small for V1 WAL header"),
            WalHeaderV1Error::UnsupportedVersion(version) => {
                write!(f, "Unsupported WAL version: {}", version)
            }
            WalHeaderV1Error::InvalidDataSizeBytes(size) => {
                write!(f, "Invalid data size bytes: {}", size)
            }
            WalHeaderV1Error::InvalidIdSizeBytes(size) => {
                write!(f, "Invalid ID size bytes: {}", size)
            }
            WalHeaderV1Error::ValueTooLarge => {
                write!(f, "Value does not fit in a u64 V1 WAL header field")
            }
            WalHeaderV1Error::UnknownStatus(status) => {
                write!(f, "Unknown V1 WAL header status: {}", status)
            }
            WalHeaderV1Error::CrcMismatch { stored, computed } => write!(
                f,
                "V1 WAL header CRC mismatch: stored {:#010x}, computed {:#010x}",
                stored, computed
            ),
        }
    }
}

impl std::error::Error for WalHeaderV1Error {}

impl From<varint::VarintError> for WalHeaderV1Error {
    fn from(e: varint::VarintError) -> Self {
        match e {
            varint::VarintError::Truncated => WalHeaderV1Error::Truncated,
            varint::VarintError::Overflow => WalHeaderV1Error::Overflow,
            varint::VarintError::NonCanonical => WalHeaderV1Error::NonCanonical,
            varint::VarintError::NoSpace => WalHeaderV1Error::NoSpace,
            varint::VarintError::UnknownStatus(status) => WalHeaderV1Error::UnknownStatus(status),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CWalHeaderV1 {
    data_size_bytes: u64,
    id_size_bytes: u64,
    num_entries_before: u64,
}

#[repr(C)]
struct CEncodeResult {
    written: usize,
    status: c_int,
}

#[repr(C)]
struct CDecodeResult {
    header: CWalHeaderV1,
    version: u64,
    consumed: usize,
    status: c_int,
}

#[repr(C)]
struct CVersionResult {
    version: u64,
    consumed: usize,
    status: c_int,
}

unsafe extern "C" {
    fn normfs_wal_header_v1_validate(header: *const CWalHeaderV1) -> c_int;

    fn normfs_wal_header_v1_size(header: *const CWalHeaderV1) -> usize;

    fn normfs_wal_header_v1_encode(
        header: *const CWalHeaderV1,
        out: *mut u8,
        out_len: usize,
    ) -> CEncodeResult;

    fn normfs_wal_header_v1_decode(buf: *const u8, len: usize) -> CDecodeResult;

    fn normfs_wal_header_peek_version(buf: *const u8, len: usize) -> CVersionResult;
}

fn map_status(status: c_int, header: &CWalHeaderV1, version: u64) -> Result<(), WalHeaderV1Error> {
    match status {
        NORMFS_WAL_HEADER_OK => Ok(()),
        NORMFS_WAL_HEADER_ERR_TRUNCATED => Err(WalHeaderV1Error::Truncated),
        NORMFS_WAL_HEADER_ERR_OVERFLOW => Err(WalHeaderV1Error::Overflow),
        NORMFS_WAL_HEADER_ERR_NON_CANONICAL => Err(WalHeaderV1Error::NonCanonical),
        NORMFS_WAL_HEADER_ERR_NO_SPACE => Err(WalHeaderV1Error::NoSpace),
        NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION => {
            Err(WalHeaderV1Error::UnsupportedVersion(version))
        }
        NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE => Err(WalHeaderV1Error::InvalidDataSizeBytes(
            header.data_size_bytes,
        )),
        NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE => {
            Err(WalHeaderV1Error::InvalidIdSizeBytes(header.id_size_bytes))
        }
        other => Err(WalHeaderV1Error::UnknownStatus(other)),
    }
}

/// Reads the version word of a header without consuming the rest of it.
///
/// Both versions carry the version as a `u64` little-endian word at offset 0,
/// which is what lets a reader tell them apart, and what lets a V0-only reader
/// reject a V1 file cleanly.
pub fn peek_version(data: &[u8]) -> Result<u64, WalHeaderV1Error> {
    let result = unsafe { normfs_wal_header_peek_version(data.as_ptr(), data.len()) };
    map_status(
        result.status,
        &CWalHeaderV1 {
            data_size_bytes: 0,
            id_size_bytes: 0,
            num_entries_before: 0,
        },
        result.version,
    )?;
    Ok(result.version)
}

/// Reads one varint off a stream without reading past its end.
///
/// Where the varint stops is decided by the verified C decoder, not here: a
/// byte is appended and handed to it, and only its `Truncated` status causes
/// another byte to be pulled. Rust supplies the I/O, the framing rules stay in
/// one place.
async fn read_varint_u64<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(u64, usize), WalHeaderV1Error> {
    let mut buf = [0u8; varint::MAX_VARINT64_LEN];
    let mut len = 0usize;

    while len < varint::MAX_VARINT64_LEN {
        reader
            .read_exact(&mut buf[len..len + 1])
            .await
            .map_err(|_| WalHeaderV1Error::Truncated)?;
        len += 1;

        match varint::decode_u64(&buf[..len]) {
            Ok(decoded) => return Ok((decoded.value, decoded.consumed)),
            Err(varint::VarintError::Truncated) => continue,
            Err(e) => return Err(e.into()),
        }
    }

    Err(WalHeaderV1Error::Overflow)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeaderV1 {
    pub data_size_bytes: u64,
    pub id_size_bytes: u64,
    pub num_entries_before: u64,
}

impl WalHeaderV1 {
    pub fn new(
        data_size_bytes: u64,
        id_size_bytes: u64,
        num_entries_before: u64,
    ) -> Result<Self, WalHeaderV1Error> {
        let candidate = CWalHeaderV1 {
            data_size_bytes,
            id_size_bytes,
            num_entries_before,
        };

        let status = unsafe { normfs_wal_header_v1_validate(&candidate) };
        map_status(status, &candidate, WAL_HEADER_V1_VERSION)?;

        Ok(Self {
            data_size_bytes,
            id_size_bytes,
            num_entries_before,
        })
    }

    /// Re-encodes a V0 header as V1. Fails when `num_entries_before` does not
    /// fit in a u64, which V1 headers are bounded to.
    pub fn from_v0(header: &WalHeader) -> Result<Self, WalHeaderV1Error> {
        let num_entries_before = header
            .num_entries_before
            .to_u64()
            .map_err(|_| WalHeaderV1Error::ValueTooLarge)?;

        Self::new(
            header.data_size_bytes,
            header.id_size_bytes,
            num_entries_before,
        )
    }

    /// Id of the `index`-th entry in this file. V1 entries do not store their
    /// id, so it is derived from the header instead.
    ///
    /// A consequence worth knowing when comparing to V0: because ids are
    /// positional, a corrupt entry ends the usable part of the file. V0 stored
    /// an id per entry, so a reader could in principle skip a bad entry and
    /// still label the ones after it; under V1 those ids are unknowable, and
    /// every scan stops at the first entry that fails to decode.
    pub fn entry_id(&self, index: u64) -> Result<u64, WalEntryV1Error> {
        derive_entry_id(self.num_entries_before, index)
    }

    /// Checks the trailer that follows `fields_len` header bytes in `data`,
    /// returning the total consumed length including it.
    fn verify_crc_trailer(data: &[u8], fields_len: usize) -> Result<usize, WalHeaderV1Error> {
        let end = fields_len + WAL_HEADER_V1_CRC_SIZE;
        if data.len() < end {
            return Err(WalHeaderV1Error::Truncated);
        }

        let stored = u32::from_le_bytes(data[fields_len..end].try_into().unwrap());
        let computed = crc32c(0, &data[..fields_len]);
        if stored != computed {
            return Err(WalHeaderV1Error::CrcMismatch { stored, computed });
        }

        Ok(end)
    }

    fn as_c(&self) -> CWalHeaderV1 {
        CWalHeaderV1 {
            data_size_bytes: self.data_size_bytes,
            id_size_bytes: self.id_size_bytes,
            num_entries_before: self.num_entries_before,
        }
    }

    pub fn size(&self) -> usize {
        let header = self.as_c();
        (unsafe { normfs_wal_header_v1_size(&header) }) + WAL_HEADER_V1_CRC_SIZE
    }

    pub fn write_to_bytes(&self, dest: &mut BytesMut) -> Result<usize, WalHeaderV1Error> {
        let header = self.as_c();
        let mut buf = [0u8; WAL_HEADER_V1_MAX_SIZE];

        let result = unsafe { normfs_wal_header_v1_encode(&header, buf.as_mut_ptr(), buf.len()) };
        map_status(result.status, &header, WAL_HEADER_V1_VERSION)?;

        let fields = &buf[..result.written];
        dest.extend_from_slice(fields);
        dest.extend_from_slice(&crc32c(0, fields).to_le_bytes());
        Ok(result.written + WAL_HEADER_V1_CRC_SIZE)
    }

    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), WalHeaderV1Error> {
        let result = unsafe { normfs_wal_header_v1_decode(data.as_ptr(), data.len()) };
        map_status(result.status, &result.header, result.version)?;

        let consumed = Self::verify_crc_trailer(data, result.consumed)?;

        Ok((
            Self {
                data_size_bytes: result.header.data_size_bytes,
                id_size_bytes: result.header.id_size_bytes,
                num_entries_before: result.header.num_entries_before,
            },
            consumed,
        ))
    }

    pub async fn from_reader<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(Self, usize), WalHeaderV1Error> {
        let mut version_word = [0u8; WAL_HEADER_VERSION_SIZE];
        reader
            .read_exact(&mut version_word)
            .await
            .map_err(|_| WalHeaderV1Error::Truncated)?;

        let version = peek_version(&version_word)?;
        if version != WAL_HEADER_V1_VERSION {
            return Err(WalHeaderV1Error::UnsupportedVersion(version));
        }

        let (data_size_bytes, data_size_len) = read_varint_u64(reader).await?;
        let (id_size_bytes, id_size_len) = read_varint_u64(reader).await?;
        let (num_entries_before, num_entries_len) = read_varint_u64(reader).await?;

        let header = Self::new(data_size_bytes, id_size_bytes, num_entries_before)?;
        let fields_len =
            WAL_HEADER_VERSION_SIZE + data_size_len + id_size_len + num_entries_len;

        // The fields have been consumed from the stream, so re-encode them to
        // recover the exact bytes the trailer was computed over rather than
        // buffering the stream twice.
        let mut fields = BytesMut::with_capacity(fields_len + WAL_HEADER_V1_CRC_SIZE);
        let c_header = header.as_c();
        let mut buf = [0u8; WAL_HEADER_V1_MAX_SIZE];
        let encoded =
            unsafe { normfs_wal_header_v1_encode(&c_header, buf.as_mut_ptr(), buf.len()) };
        map_status(encoded.status, &c_header, WAL_HEADER_V1_VERSION)?;
        fields.extend_from_slice(&buf[..encoded.written]);

        let mut trailer = [0u8; WAL_HEADER_V1_CRC_SIZE];
        reader
            .read_exact(&mut trailer)
            .await
            .map_err(|_| WalHeaderV1Error::Truncated)?;
        fields.extend_from_slice(&trailer);

        Self::verify_crc_trailer(&fields, encoded.written)?;

        Ok((header, fields_len + WAL_HEADER_V1_CRC_SIZE))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AnyWalHeaderError {
    V0(WalHeaderError),
    V1(WalHeaderV1Error),
}

impl std::fmt::Display for AnyWalHeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyWalHeaderError::V0(e) => write!(f, "{}", e),
            AnyWalHeaderError::V1(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for AnyWalHeaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AnyWalHeaderError::V0(e) => Some(e),
            AnyWalHeaderError::V1(e) => Some(e),
        }
    }
}

impl From<WalHeaderError> for AnyWalHeaderError {
    fn from(e: WalHeaderError) -> Self {
        AnyWalHeaderError::V0(e)
    }
}

impl From<WalHeaderV1Error> for AnyWalHeaderError {
    fn from(e: WalHeaderV1Error) -> Self {
        AnyWalHeaderError::V1(e)
    }
}

/// A WAL header of either on-disk version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyWalHeader {
    V0(WalHeader),
    V1(WalHeaderV1),
}

impl AnyWalHeader {
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), AnyWalHeaderError> {
        if peek_version(data)? == WAL_HEADER_V0_VERSION {
            let (header, bytes_read) = WalHeader::from_bytes(data)?;
            return Ok((AnyWalHeader::V0(header), bytes_read));
        }

        let (header, bytes_read) = WalHeaderV1::from_bytes(data)?;
        Ok((AnyWalHeader::V1(header), bytes_read))
    }

    pub async fn from_reader<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(Self, usize), AnyWalHeaderError> {
        let mut version_word = [0u8; WAL_HEADER_VERSION_SIZE];
        reader
            .read_exact(&mut version_word)
            .await
            .map_err(|_| WalHeaderV1Error::Truncated)?;

        let version = peek_version(&version_word)?;
        let mut reader = Cursor::new(version_word).chain(reader);

        if version == WAL_HEADER_V0_VERSION {
            let (header, bytes_read) = WalHeader::from_reader(&mut reader).await?;
            return Ok((AnyWalHeader::V0(header), bytes_read));
        }

        let (header, bytes_read) = WalHeaderV1::from_reader(&mut reader).await?;
        Ok((AnyWalHeader::V1(header), bytes_read))
    }

    pub fn version(&self) -> u64 {
        match self {
            AnyWalHeader::V0(_) => WAL_HEADER_V0_VERSION,
            AnyWalHeader::V1(_) => WAL_HEADER_V1_VERSION,
        }
    }

    pub fn data_size_bytes(&self) -> u64 {
        match self {
            AnyWalHeader::V0(header) => header.data_size_bytes,
            AnyWalHeader::V1(header) => header.data_size_bytes,
        }
    }

    pub fn id_size_bytes(&self) -> u64 {
        match self {
            AnyWalHeader::V0(header) => header.id_size_bytes,
            AnyWalHeader::V1(header) => header.id_size_bytes,
        }
    }

    pub fn num_entries_before(&self) -> UintN {
        match self {
            AnyWalHeader::V0(header) => header.num_entries_before.clone(),
            AnyWalHeader::V1(header) => UintN::from(header.num_entries_before),
        }
    }
}

/// Projects either version onto the three fields that entry parsing needs.
///
/// The entry layout does not depend on the header version, only on the two
/// field widths and the running entry count, and both versions carry all
/// three. Readers dispatch on the version once and then work with this view,
/// so nothing downstream of the header has to know which version it came from.
impl From<&AnyWalHeader> for WalHeader {
    fn from(header: &AnyWalHeader) -> Self {
        match header {
            AnyWalHeader::V0(v0) => v0.clone(),
            AnyWalHeader::V1(v1) => WalHeader {
                data_size_bytes: v1.data_size_bytes,
                id_size_bytes: v1.id_size_bytes,
                num_entries_before: UintN::from(v1.num_entries_before),
            },
        }
    }
}
