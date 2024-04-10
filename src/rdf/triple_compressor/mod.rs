pub mod compressor;
pub mod decompressor;

use crate::MemoryMapped;
use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

pub const COMPRESSED_TRIPLE_FILE_EXTENSION: &str = "compressed_nt";
pub const UNCOMPRESSED_TRIPLE_FILE_EXTENSION: &str = "nt";

pub type TripleId = u64;
pub type TripleElementId = u64;
pub type RawTriple<'a> = [&'a [u8]; 3];
pub type CompressedTriple = [TripleElementId; 3];

pub struct CompressedRdfTriples(MemoryMapped<[CompressedTriple]>);

impl CompressedRdfTriples {
    pub unsafe fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Ok(CompressedRdfTriples(MemoryMapped::open_slice(path)?.assume_init()))
    }

    pub unsafe fn load_shared<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Ok(CompressedRdfTriples(
            MemoryMapped::options()
                .read(true)
                .write(true)
                .open_shared_slice(path)?
                .assume_init(),
        ))
    }

    pub fn contains(&self, triple: &CompressedTriple) -> bool {
        self.0.binary_search(triple).is_ok()
    }
}

impl Deref for CompressedRdfTriples {
    type Target = MemoryMapped<[CompressedTriple]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CompressedRdfTriples {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> IntoIterator for &'a CompressedRdfTriples {
    type Item = &'a CompressedTriple;
    type IntoIter = std::slice::Iter<'a, CompressedTriple>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
