use crate::{
    rdf::triple_compressor::{CompressedRdfTriples, CompressedTriple, TripleElementId},
};
use rand::{Rng, SeedableRng};
use std::collections::HashSet;

pub fn random_distinct_triple_generator(
    triples: &CompressedRdfTriples,
    n_total_query_triples: usize,
) -> impl FnMut(usize) -> Vec<CompressedTriple> + '_ {
    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut ixs = rand::seq::index::sample(&mut rng, triples.len(), n_total_query_triples).into_vec();
    ixs.sort_unstable();
    let mut itr = ixs.into_iter();

    move |size_hint: usize| {
        let mut buf = Vec::with_capacity(size_hint);
        for _ in 0..size_hint {
            let Some(ix) = itr.next() else {
                break;
            };

            buf.push(triples[ix]);
        }

        buf
    }
}

pub fn random_triple_generator(triples: &CompressedRdfTriples) -> impl FnMut(usize) -> Vec<CompressedTriple> + '_ {
    let mut rng = rand::rngs::StdRng::from_entropy();

    move |size_hint: usize| {
        let mut ixs = rand::seq::index::sample(&mut rng, triples.len(), size_hint).into_vec();
        ixs.sort_unstable();

        ixs.into_iter()
            .map(|ix| triples[ix])
            .collect()
    }
}

pub fn fixed_size_changeset_triple_generator<'a, 'c, 'd>(
    changesets: &'c [CompressedRdfTriples],
    dataset: &'d CompressedRdfTriples,
) -> impl FnMut(usize) -> Box<dyn Iterator<Item = &'c [TripleElementId; 3]> + Send + 'a>
where
    'c: 'a,
    'd: 'a,
{
    let start_off = rand::thread_rng().gen_range(0..changesets.len());

    move |size_hint: usize| {
        let itr = changesets[start_off..]
            .iter()
            .chain(changesets[..start_off].iter().rev())
            .flat_map(|compressed_triples| compressed_triples.iter())
            .filter(|triple| dataset.contains(triple))
            .take(size_hint);

        Box::new(itr)
    }
}

pub fn as_is_changeset_triple_generator<'c>(
    changesets: &'c [CompressedRdfTriples],
) -> impl FnMut(usize) -> Box<dyn Iterator<Item = &'c [TripleElementId; 3]> + Send + 'c> {
    let mut used = HashSet::new();

    move |size_hint: usize| {
        let (used_ix, changeset) = changesets
            .iter()
            .enumerate()
            .filter(|(ix, _)| !used.contains(ix))
            .min_by_key(|(_, triples)| triples.len().abs_diff(size_hint))
            .expect("more than 0 changesets");

        println!("using changeset: {used_ix}");

        used.insert(used_ix);

        Box::new(changeset.iter())
    }
}
