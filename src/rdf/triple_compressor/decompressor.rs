use super::CompressedRdfTriples;
use crate::rdf::triple_compressor::{CompressedTriple, RawTriple, TripleElementId};
use memory_mapped::MemoryMapped;
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

pub struct RdfTripleDecompressor {
    pub(super) header: MemoryMapped<[(TripleElementId, usize, usize)]>,
    pub(super) data_segment: MemoryMapped<[u8]>,
}

impl RdfTripleDecompressor {
    fn search_header(&self, hash: TripleElementId) -> Option<&(TripleElementId, usize, usize)> {
        let ix = self.header.binary_search_by_key(&hash, |(h, _, _)| *h).ok()?;
        Some(&self.header[ix])
    }

    pub unsafe fn load_state<P: AsRef<Path>>(path: P) -> std::io::Result<RdfTripleDecompressor> {
        let header_size = {
            let mut f = File::open(path.as_ref())?;

            let mut header_size_buf = [0; std::mem::size_of::<usize>()];
            f.read_exact(&mut header_size_buf)?;

            usize::from_ne_bytes(header_size_buf)
        };

        let header = MemoryMapped::options()
            .read(true)
            .byte_offset(std::mem::size_of::<usize>())
            .byte_len(header_size)
            .open_slice(path.as_ref())?
            .assume_init();

        let data_segment = MemoryMapped::options()
            .read(true)
            .byte_offset(std::mem::size_of::<usize>() + header_size)
            .open_slice(path.as_ref())?
            .assume_init();

        Ok(Self { header, data_segment })
    }

    pub fn decompress_rdf_triple(&self, [subject, predicate, object]: CompressedTriple) -> Option<RawTriple> {
        let &(_, s_start, s_end) = self.search_header(subject)?;
        let &(_, p_start, p_end) = self.search_header(predicate)?;
        let &(_, o_start, o_end) = self.search_header(object)?;

        Some([
            &self.data_segment[s_start..s_end],
            &self.data_segment[p_start..p_end],
            &self.data_segment[o_start..o_end],
        ])
    }

    pub fn decompress_rdf_triple_file<P: AsRef<Path>, W: Write>(&self, path: P, mut out: W) -> std::io::Result<()> {
        let in_triples = unsafe { CompressedRdfTriples::load(path)? };

        for &triple in in_triples.iter() {
            let [s, p, o] = self
                .decompress_rdf_triple(triple)
                .expect("using same compressor state for compression and decompression");

            out.write_all(s)?;
            out.write_all(b" ")?;
            out.write_all(p)?;
            out.write_all(b" ")?;
            out.write_all(o)?;
            out.write_all(b" .\n")?;
        }

        Ok(())
    }
}
