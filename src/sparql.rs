use crate::{
    rdf::triple_compressor::{decompressor::RdfTripleDecompressor, RawTriple},
    CompressedRdfTriples, OutputFormat,
};
use clap::ArgEnum;
use rand::seq::SliceRandom;
use std::{
    borrow::Borrow,
    fs::File,
    hash::Hash,
    io,
    io::{BufWriter, Write},
    path::Path,
};
use crate::rdf::triple_compressor::CompressedTriple;

#[derive(Copy, Clone, ArgEnum)]
pub enum OutputOrder {
    AsSpecified,
    Randomized,
    SortedSizeAsc,
    SortedSizeDesc,
    SortedSizeAscAlternateInsertDelete,
}

#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum QueryType {
    InsertData,
    DeleteData,
}

#[derive(Clone, Copy)]
pub struct QuerySpec {
    pub n_queries: usize,
    pub n_triples_per_query: usize,
    pub query_type: QueryType,
}

pub fn generate_queries<P, P2, Q, F, I, T>(
    out_query: P,
    out_prepare: P2,
    prepare_format: OutputFormat,
    query_specs: Q,
    decompressor: &RdfTripleDecompressor,
    mut triple_generator_factory: F,
    order: OutputOrder,
    append: bool,
) -> io::Result<()>
where
    P: AsRef<Path>,
    P2: AsRef<Path>,
    Q: IntoIterator<Item = QuerySpec>,
    F: FnMut(usize) -> I,
    I: IntoIterator<Item = T>,
    T: Borrow<[u64; 3]> + Eq + Hash,
{
    let generators: Vec<_> = {
        let mut tmp: Vec<_> = query_specs
            .into_iter()
            .flat_map(|QuerySpec { n_queries, n_triples_per_query, query_type }| {
                std::iter::repeat((n_triples_per_query, query_type)).take(n_queries)
            })
            .collect();

        match order {
            OutputOrder::AsSpecified => (),
            OutputOrder::Randomized => tmp.shuffle(&mut rand::thread_rng()),
            OutputOrder::SortedSizeAsc => tmp.sort_by_key(|&(size, _)| size),
            OutputOrder::SortedSizeDesc => tmp.sort_by_key(|&(size, _)| std::cmp::Reverse(size)),
            OutputOrder::SortedSizeAscAlternateInsertDelete => {
                if tmp.len() % 2 != 0 {
                    eprintln!("Error: need even number of queries to be able to sort as alternating");
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "need even number of queries to be able to sort as alternating",
                    ));
                }

                tmp.sort_unstable();

                let (ins, del): (Vec<_>, Vec<_>) = tmp
                    .into_iter()
                    .partition(|&(_, query_type)| query_type == QueryType::InsertData);

                tmp = ins.into_iter().zip(del).flat_map(|(i, d)| [i, d]).collect();
            },
        }

        tmp
    };

    let queries = generators.into_iter().map(|(n_triples, query_type)| {
        let triple_set = triple_generator_factory(n_triples).into_iter().map(|triple| {
            decompressor
                .decompress_rdf_triple(*triple.borrow())
                .expect("to use same compressor as used for compression")
        });

        (query_type, Some(n_triples), triple_set)
    });

    write_update_data_queries(out_query, Some((out_prepare, prepare_format)), append, queries)
}

pub fn generate_linear_no_size_hint<P, F, I, T>(
    out_file: P,
    decompressor: &RdfTripleDecompressor,
    exclude_dataset: Option<&CompressedRdfTriples>,
    generators: F,
    append: bool,
    output_format: OutputFormat,
) -> io::Result<()>
where
    P: AsRef<Path>,
    F: IntoIterator<Item = (QueryType, I)>,
    I: IntoIterator<Item = T>,
    T: Borrow<CompressedTriple> + Eq + Hash,
{
    let queries: Vec<_> = generators
        .into_iter()
        .map(|(query_type, triple_generator)| {
            let triples = triple_generator
                .into_iter()
                .filter(|triple| exclude_dataset.map(|exclude| !exclude.contains(triple.borrow())).unwrap_or(true))
                .map(|triple| {
                    decompressor
                        .decompress_rdf_triple(*triple.borrow())
                        .expect("to use same compressor as used for compression")
                });

            (query_type, None, triples)
        })
        .collect();

    match output_format {
        OutputFormat::Query => write_update_data_queries(out_file, None::<(&Path, OutputFormat)>, append, queries),
        OutputFormat::NTriples => write_ntriples_file(out_file, append, queries),
    }
}

fn write_update_data_queries<'a, P, P2, I>(
    out_file: P,
    prepare_out_file: Option<(P2, OutputFormat)>,
    append: bool,
    queries: impl IntoIterator<Item = (QueryType, Option<usize>, I)>,
) -> io::Result<()>
where
    P: AsRef<Path>,
    P2: AsRef<Path>,
    I: Iterator<Item = RawTriple<'a>>,
{
    let f = File::options()
        .append(append)
        .truncate(!append)
        .create(true)
        .write(true)
        .open(out_file)?;

    let mut writer = BufWriter::new(f);

    let mut prepare_writer = if let Some((prepare_out_file, prepare_format)) = prepare_out_file {
        let prepare_f = File::options()
            .append(append)
            .truncate(!append)
            .create(true)
            .write(true)
            .open(prepare_out_file)?;

        Some((BufWriter::new(prepare_f), prepare_format))
    } else {
        None
    };

    let write_query = |out: &mut BufWriter<File>,
                       mut prepare_out: Option<&mut (BufWriter<File>, OutputFormat)>,
                       expected_n_triples: Option<usize>,
                       query: I|
     -> io::Result<()> {
        let mut cnt = 0;

        if let Some((prepare_out, prepare_format)) = &mut prepare_out {
            out.write_all(b"INSERT DATA { ")?;

            if *prepare_format == OutputFormat::Query {
                prepare_out.write_all(b"DELETE DATA { ")?;
            }

            for [s, p, o] in query {
                out.write_all(s)?;
                out.write_all(b" ")?;
                out.write_all(p)?;
                out.write_all(b" ")?;
                out.write_all(o)?;
                out.write_all(b" . ")?;

                prepare_out.write_all(s)?;
                prepare_out.write_all(b" ")?;
                prepare_out.write_all(p)?;
                prepare_out.write_all(b" ")?;
                prepare_out.write_all(o)?;

                if *prepare_format == OutputFormat::Query {
                    prepare_out.write_all(b" . ")?;
                } else {
                    prepare_out.write_all(b" .\n")?;
                }

                cnt += 1;
            }

            out.write_all(b"}\n")?;

            if *prepare_format == OutputFormat::Query {
                prepare_out.write_all(b"}\n")?;
            }
        } else {
            out.write_all(b"DELETE DATA { ")?;

            for [s, p, o] in query {
                out.write_all(s)?;
                out.write_all(b" ")?;
                out.write_all(p)?;
                out.write_all(b" ")?;
                out.write_all(o)?;
                out.write_all(b" . ")?;

                cnt += 1;
            }

            out.write_all(b"}\n")?;
        }

        if let Some(expected_n_triples) = expected_n_triples {
            if cnt != expected_n_triples {
                println!("Warning: requested query size {expected_n_triples} cannot be fulfilled closest available size is {cnt}");
            }
        }

        Ok(())
    };

    for (query_type, n_triples, query) in queries {
        match query_type {
            QueryType::DeleteData => {
                write_query(&mut writer, None, n_triples, query)?;
            },
            QueryType::InsertData => {
                write_query(&mut writer, prepare_writer.as_mut(), n_triples, query)?;
            },
        }
    }

    Ok(())
}

fn write_ntriples_file<'a, P, I>(
    out_file: P,
    append: bool,
    queries: impl IntoIterator<Item = (QueryType, Option<usize>, I)>,
) -> io::Result<()>
where
    P: AsRef<Path>,
    I: Iterator<Item = RawTriple<'a>>,
{
    let f = File::options()
        .append(append)
        .truncate(!append)
        .create(true)
        .write(true)
        .open(out_file)?;

    let mut writer = BufWriter::new(f);

    let write_ntriples = |out: &mut BufWriter<File>, expected_n_triples: Option<usize>, query: I| -> io::Result<()> {
        let mut cnt = 0;

        for [s, p, o] in query {
            out.write_all(s)?;
            out.write_all(b" ")?;
            out.write_all(p)?;
            out.write_all(b" ")?;
            out.write_all(o)?;
            out.write_all(b" .\n")?;

            cnt += 1;
        }

        if let Some(expected_n_triples) = expected_n_triples {
            if cnt != expected_n_triples {
                println!("Warning: requested query size {expected_n_triples} cannot be fulfilled closest available size is {cnt}");
            }
        }

        Ok(())
    };

    for (_query_type, n_triples, query) in queries {
        write_ntriples(&mut writer, n_triples, query)?;
    }

    Ok(())
}
