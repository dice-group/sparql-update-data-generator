use super::TripleElementId;
use crate::rdf::triple_compressor::{CompressedTriple, RawTriple, TripleId};
use rio_api::{
    model::{Subject, Term, Triple},
    parser::TriplesParser,
};
use rio_turtle::NTriplesParser;
use std::{
    collections::{BTreeMap, HashSet},
    fs::{File, OpenOptions},
    hash::{BuildHasher, BuildHasherDefault, Hash},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

fn hash_single<T: Hash>(to_hash: T) -> u64 {
    type BuildH = BuildHasherDefault<ahash::AHasher>;
    BuildH::default().hash_one(to_hash)
}

#[derive(Default)]
pub struct RdfTripleCompressor {
    translations: BTreeMap<TripleElementId, Vec<u8>>,
    dedup: HashSet<TripleId, BuildHasherDefault<ahash::AHasher>>,
}

impl RdfTripleCompressor {
    fn found_new_triple(&mut self, triple: [TripleElementId; 3]) -> bool {
        let hash = hash_single(triple);
        self.dedup.insert(hash)
    }
}

impl RdfTripleCompressor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn save_state<P: AsRef<Path>>(&mut self, path: P) -> std::io::Result<()> {
        let header_size = self.translations.len() * std::mem::size_of::<(TripleElementId, usize, usize)>();

        let f = OpenOptions::new().write(true).create(true).truncate(true).open(&path)?;
        let mut bw = BufWriter::new(f);

        bw.write_all(&header_size.to_ne_bytes())?;

        let mut data_segment_off: usize = 0;
        for (hash, rdf_str) in &self.translations {
            bw.write_all(&hash.to_ne_bytes())?;
            bw.write_all(&data_segment_off.to_ne_bytes())?;

            data_segment_off += rdf_str.len();
            bw.write_all(&data_segment_off.to_ne_bytes())?;
        }

        for rdf_str in self.translations.values() {
            bw.write_all(rdf_str)?;
        }

        Ok(())
    }

    pub fn from_decompressor(frozen: super::decompressor::RdfTripleDecompressor) -> Self {
        let mut translations = BTreeMap::default();

        for (hash, s_beg, s_end) in frozen.header {
            let rdf_data = frozen.data_segment[s_beg..s_end].to_owned();

            translations.insert(hash, rdf_data);
        }

        Self { translations, dedup: HashSet::default() }
    }

    pub fn compress_parsed_rdf_triple(&mut self, Triple { subject, predicate, object }: Triple) -> [TripleElementId; 3] {
        let subject = subject.to_string().into_bytes();
        let predicate = predicate.to_string().into_bytes();
        let object = object.to_string().into_bytes();

        let subject_hash = hash_single(&subject);
        let predicate_hash = hash_single(&predicate);
        let object_hash = hash_single(&object);

        self.translations
            .entry(subject_hash)
            .or_insert(subject);
        self.translations
            .entry(predicate_hash)
            .or_insert(predicate);
        self.translations
            .entry(object_hash)
            .or_insert(object);

        [subject_hash, predicate_hash, object_hash]
    }

    pub fn compress_raw_rdf_triple(&mut self, [subject, predicate, object]: RawTriple) -> [TripleElementId; 3] {
        let subject_hash = hash_single(subject);
        let predicate_hash = hash_single(predicate);
        let object_hash = hash_single(object);

        self.translations
            .entry(subject_hash)
            .or_insert_with(|| subject.to_owned());
        self.translations
            .entry(predicate_hash)
            .or_insert_with(|| predicate.to_owned());
        self.translations
            .entry(object_hash)
            .or_insert_with(|| object.to_owned());

        [subject_hash, predicate_hash, object_hash]
    }

    fn compress_parsed_rdf_triple_file<R: BufRead>(
        &mut self,
        dedup: bool,
        tx: std::sync::mpsc::Sender<CompressedTriple>,
        mut parser: NTriplesParser<R>,
    ) -> std::io::Result<()> {
        while !parser.is_end() {
            let res: Result<(), std::io::Error> = parser.parse_step(&mut |triple| {
                let subject @ Subject::NamedNode(_) = triple.subject else {
                    return Ok(());
                };

                let predicate = triple.predicate;

                let object @ (Term::NamedNode(_) | Term::Literal(_)) = triple.object else {
                    return Ok(());
                };

                let triple = self.compress_parsed_rdf_triple(Triple { subject, predicate, object });

                if !dedup || self.found_new_triple(triple) {
                    tx.send(triple).unwrap();
                }

                Ok(())
            });

            if let Err(e) = res {
                eprintln!("{e}")
            }
        }

        Ok(())
    }

    fn compress_raw_rdf_triple_file<R: BufRead>(
        &mut self,
        dedup: bool,
        tx: std::sync::mpsc::Sender<CompressedTriple>,
        reader: R,
    ) -> std::io::Result<()> {
        for line in reader.split(b'\n') {
            let line = line?;

            if line.is_empty() || line.starts_with(b"#") {
                continue;
            }

            let mut split = line.splitn(3, |&b| b == b' ');

            let subject = split.next().unwrap();
            let predicate = split.next().unwrap();
            let object = split.next().unwrap();

            assert!(object.ends_with(b" ."));
            let object = &object[..object.len() - 2];

            if subject.starts_with(b"_") | object.starts_with(b"_") {
                continue;
            }

            let triple = self.compress_raw_rdf_triple([subject, predicate, object]);

            if !dedup || self.found_new_triple(triple) {
                tx.send(triple).unwrap();
            }
        }

        Ok(())
    }

    pub fn compress_rdf_triple_file<P: AsRef<Path>>(
        &mut self,
        path: P,
        dedup: bool,
        parse: bool,
    ) -> std::io::Result<()> {
        let out_path = path.as_ref().with_extension(super::COMPRESSED_TRIPLE_FILE_EXTENSION);

        let mut bw = BufWriter::new(File::options().write(true).create_new(true).open(out_path)?);
        let input_triples = BufReader::new(File::open(path)?);

        let (writer_res, reader_res) = std::thread::scope(move |s| {
            let (tx, rx) = std::sync::mpsc::channel::<[TripleElementId; 3]>();

            let writer = s.spawn(move || -> std::io::Result<()> {
                while let Ok([s, p, o]) = rx.recv() {
                    bw.write_all(&s.to_ne_bytes())?;
                    bw.write_all(&p.to_ne_bytes())?;
                    bw.write_all(&o.to_ne_bytes())?;
                }

                Ok(())
            });

            let reader = if parse {
                s.spawn(move || -> std::io::Result<()> {
                    self.compress_parsed_rdf_triple_file(dedup, tx, NTriplesParser::new(input_triples))
                })
            } else {
                s.spawn(move || -> std::io::Result<()> { self.compress_raw_rdf_triple_file(dedup, tx, input_triples) })
            };

            (writer.join(), reader.join())
        });

        writer_res.unwrap()?;
        reader_res.unwrap()?;

        Ok(())
    }
}
