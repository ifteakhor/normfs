//! What a queue's records would cost on disk if every memory page were its
//! own store file.
//!
//!   store_repack <corpus> [page_size...]      default: 32K 256K 1M 4M 16M
//!
//! The corpus is `<u32 le len><bytes>` per record, as `store_corpus` and
//! `corpus_extract` write it. Records are framed as V1 entries and laid onto
//! pages by the pool's own rule (`normfs_wal_page_append`: an entry needs its
//! bytes plus a 4-byte offset slot), so a page closes exactly where the real
//! one would. Each closed page is compressed the way `compression.rs` does it
//! and charged the fixed cost of a store file: authentication, store header,
//! nonce and tag. Page tails never reach disk, in either mode.
//!
//! The `wal128M` row is today's layout -- a file closing at the first page
//! after 128 MB, on the widest page asked for -- so the other rows read as a
//! ratio against it.
//!
//! Every layout is fed from one pass over the corpus; a corpus of several GB
//! is read once and compressed once per layout.

use std::io::{BufReader, Read, Write};
use std::time::Instant;

use bytes::BytesMut;
use normfs_wal::{
    WAL_HEADER_V1_MAX_SIZE, WAL_HEADER_V1_MIN_SIZE, WalEntryV1, encoded_len, max_record_len,
};

/// FileAuthentication (152) + StoreHeaderV1 (at most 48) + AES-GCM nonce and tag.
const STORE_FILE_OVERHEAD: u64 = 152 + 48 + 12 + 16;
const FS_BLOCK: u64 = 4096;
const PAGE_ENTRY_SLOT: usize = 4;

struct Layout {
    name: String,
    page_size: usize,
    /// 0 means a page is a file.
    max_file: u64,

    page: Vec<u8>,
    page_entries: usize,
    file: Vec<u8>,
    file_pages: usize,

    files: u64,
    raw_bytes: u64,
    compressed: u64,
    on_disk: u64,
    smallest_file: u64,
    largest_file: u64,
    skipped_records: u64,
    skipped_bytes: u64,
    compress_secs: f64,
}

impl Layout {
    fn new(name: &str, page_size: usize, max_file: u64) -> Self {
        Self {
            name: name.to_string(),
            page_size,
            max_file,
            page: Vec::with_capacity(page_size),
            page_entries: 0,
            file: Vec::new(),
            file_pages: 0,
            files: 0,
            raw_bytes: 0,
            compressed: 0,
            on_disk: 0,
            smallest_file: u64::MAX,
            largest_file: 0,
            skipped_records: 0,
            skipped_bytes: 0,
            compress_secs: 0.0,
        }
    }

    fn fits(&self, entry_len: usize) -> bool {
        self.page.len() + PAGE_ENTRY_SLOT * self.page_entries + entry_len + PAGE_ENTRY_SLOT
            <= self.page_size
    }

    fn push(&mut self, frame: &[u8]) {
        if frame.len() > self.page_size {
            self.skipped_records += 1;
            self.skipped_bytes += frame.len() as u64;
            return;
        }
        if !self.fits(frame.len()) {
            self.close_page();
        }
        self.page.extend_from_slice(frame);
        self.page_entries += 1;
        self.raw_bytes += frame.len() as u64;
    }

    fn close_page(&mut self) {
        if self.page_entries == 0 {
            return;
        }
        // Rotation happens at the first page to open after the threshold,
        // so the page that crossed it stays in this file.
        if self.max_file > 0 && self.file_pages > 0 && self.file.len() as u64 >= self.max_file {
            self.close_file();
        }
        self.file.extend_from_slice(&self.page);
        self.file_pages += 1;
        self.page.clear();
        self.page_entries = 0;
        if self.max_file == 0 {
            self.close_file();
        }
    }

    fn close_file(&mut self) {
        if self.file_pages == 0 {
            return;
        }
        let started = Instant::now();
        let compressed = compress(&self.file);
        self.compress_secs += started.elapsed().as_secs_f64();

        let size = STORE_FILE_OVERHEAD + compressed as u64;
        self.files += 1;
        self.compressed += size;
        self.on_disk += size.div_ceil(FS_BLOCK) * FS_BLOCK;
        self.smallest_file = self.smallest_file.min(size);
        self.largest_file = self.largest_file.max(size);
        self.file.clear();
        self.file_pages = 0;
    }

    fn finish(&mut self) {
        self.close_page();
        self.close_file();
    }
}

/// As `compression.rs`: level 10, 128 MiB window, long-distance matching.
/// The WAL header is inside the compressed region, so it goes in here too.
fn compress(body: &[u8]) -> usize {
    let mut encoder = zstd::Encoder::new(Vec::new(), 10).unwrap();
    encoder.window_log(27).unwrap();
    encoder.long_distance_matching(true).unwrap();
    encoder.write_all(&[0u8; WAL_HEADER_V1_MAX_SIZE]).unwrap();
    encoder.write_all(body).unwrap();
    encoder.finish().unwrap().len()
}

/// The shipped codec, so the frame is byte-for-byte what a page holds.
fn frame(record: &[u8], out: &mut BytesMut) {
    out.clear();
    WalEntryV1::new(record).write_to_bytes(out).unwrap();
    debug_assert_eq!(out.len(), encoded_len(record.len() as u32));
}

fn parse_size(s: &str) -> Option<usize> {
    let (num, mul) = match s.chars().last()? {
        'K' | 'k' => (&s[..s.len() - 1], 1024),
        'M' | 'm' => (&s[..s.len() - 1], 1024 * 1024),
        'G' | 'g' => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    num.parse::<usize>().ok().map(|n| n * mul)
}

fn human(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "K", "M", "G", "T"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n}B")
    } else {
        format!("{v:.1}{}", UNITS[u])
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: store_repack <corpus> [page_size...]");
        std::process::exit(2);
    }
    let sizes: Vec<String> = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        ["32K", "256K", "1M", "4M", "16M"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    };

    let mut pages = Vec::new();
    for s in &sizes {
        let Some(page) = parse_size(s) else {
            eprintln!("bad page size: {s}");
            std::process::exit(2);
        };
        pages.push((s.clone(), page));
    }
    // The baseline takes the widest page asked for, not the 256 KiB default:
    // a corpus of 2.5 MB thermal frames fits no 256 KiB page, and a baseline
    // that skipped every record would make every ratio meaningless.
    let base_page = pages.iter().map(|(_, p)| *p).max().unwrap_or(256 * 1024);
    let mut layouts = vec![Layout::new("wal128M", base_page, 128 * 1024 * 1024)];
    for (name, page) in &pages {
        layouts.push(Layout::new(name, *page, 0));
    }
    for l in &layouts {
        assert!(
            max_record_len(l.page_size) > 0,
            "page {} cannot hold a record",
            l.name
        );
    }

    let file = std::fs::File::open(&args[0]).expect("corpus");
    let total = file.metadata().map(|m| m.len()).unwrap_or(0);
    let mut reader = BufReader::with_capacity(8 << 20, file);
    let mut len_buf = [0u8; 4];
    let mut record = Vec::new();
    let mut framed = BytesMut::new();
    let (mut records, mut record_bytes, mut read) = (0u64, 0u64, 0u64);
    let (mut smallest, mut largest) = (usize::MAX, 0usize);
    let started = Instant::now();
    let mut last_report = Instant::now();

    loop {
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("corpus: {e}"),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        record.resize(len, 0);
        reader.read_exact(&mut record).expect("corpus record");
        records += 1;
        record_bytes += len as u64;
        read += 4 + len as u64;
        smallest = smallest.min(len);
        largest = largest.max(len);

        frame(&record, &mut framed);
        for l in &mut layouts {
            l.push(&framed);
        }

        if last_report.elapsed().as_secs() >= 5 {
            eprintln!(
                "{} / {} ({:.0}%), {} records, {:.0}s",
                human(read),
                human(total),
                if total > 0 {
                    read as f64 * 100.0 / total as f64
                } else {
                    0.0
                },
                records,
                started.elapsed().as_secs_f64()
            );
            last_report = Instant::now();
        }
    }
    for l in &mut layouts {
        l.finish();
    }

    println!(
        "corpus: {} records, {} payload, record {}..{} bytes, {:.0}s",
        records,
        human(record_bytes),
        if smallest == usize::MAX { 0 } else { smallest },
        largest,
        started.elapsed().as_secs_f64()
    );
    println!(
        "per file: {} bytes fixed (auth 152, header <=48, nonce 12, tag 16) + \
         {}..{} bytes WAL header inside the compressed body; fs block {}",
        STORE_FILE_OVERHEAD, WAL_HEADER_V1_MIN_SIZE, WAL_HEADER_V1_MAX_SIZE, FS_BLOCK
    );
    println!();
    println!(
        "{:<8} {:>9} {:>10} {:>10} {:>11} {:>9} {:>9} {:>8} {:>9} {:>7} {:>13}",
        "layout",
        "files",
        "raw",
        "compressed",
        "on-disk(4K)",
        "avg/file",
        "min/file",
        "vs-base",
        "skipped",
        "zstd-s",
        "bytes"
    );
    let base = layouts[0].compressed.max(1) as f64;
    for l in &layouts {
        let avg = if l.files > 0 {
            l.compressed / l.files
        } else {
            0
        };
        println!(
            "{:<8} {:>9} {:>10} {:>10} {:>11} {:>9} {:>9} {:>7.3}x {:>9} {:>7.0} {:>13}",
            l.name,
            l.files,
            human(l.raw_bytes),
            human(l.compressed),
            human(l.on_disk),
            human(avg),
            human(if l.files > 0 { l.smallest_file } else { 0 }),
            l.compressed as f64 / base,
            if l.skipped_records > 0 {
                format!("{}/{}", l.skipped_records, human(l.skipped_bytes))
            } else {
                "-".to_string()
            },
            l.compress_secs,
            l.compressed,
        );
    }
}
