# SPARQL UPDATE DATA generator

## Generating random queries from a dataset (example: wikidata)
**Dataset**: wikidata-2020-11-11-truthy-BETA

### Download dataset
```shell
wget https://files.dice-research.org/datasets/Wikidata/2020-11-11-truthy-BETA/wikidata-2020-11-11-truthy-BETA.nt.bz2
bunzip2 wikidata-2020-11-11-truthy-BETA.nt.bz2
```

### Resulting directory structure
```cs
📁 ./
|__ 📃 wikidata-2020-11-11-truthy-BETA.nt
```

### Generate queries
```shell
# compress dataset (this step will take very long and use a lot of RAM (> 128GB))
sparql_delete_data_generator compress -D -o wikidata-dataset.compressor_state wikidata-2020-11-11-truthy-BETA.nt

# generate random DELETE DATA and INSERT DATA queries of the given sizes
# 10000 INSERT DATA queries of size 10, 10000 DELETE DATA queries of size 10, ...
sparql-update-data-generator generate -i wikidata-2020-11-11-truthy-BETA.compressed_nt -s wikidata-dataset.compressor_state \
    -o wikidata-test-queries.txt -O wikidata-preparation-queries.txt randomized \
    i10000x10 d10000x10 i10000x100 d10000x100 i10000x1000 d10000x1000 i1000x10000 d1000x10000 i100x100000 d100x100000 i10x1000000 d10x1000000
```


## Generating queries from changelogs (example: dbpedia)

**Dataset**: dbpedia 2015-10
**Changelogs**: 2015-10-*

### Download dataset
```shell
wget https://hobbitdata.informatik.uni-leipzig.de/ISWC2020_Tentris/dbpedia_2015-10_en_wo-comments_c.nt.zst
unzstd dbpedia_2015-10_en_wo-comments_c.nt.zst
```

### Download changesets
```shell
wget --no-verbose --no-parent --recursive --level inf --accept "*added.nt.gz" --accept "*removed.nt.gz" https://downloads.dbpedia.org/live/changesets/2015/10/01
find -type f downloads.dbpedia.org -name "*.gz" | xargs -n1 gunzip
```

### Resulting directory structure
```cs
📁 ./
|__ 📃 dbpedia_2015-10_en_wo-comments_c.nt
|__ 📁 downloads.dbpedia.org
|   |__ 📁 live
|   |   |__ 📁 changesets
|   |   |   |__ 📁 2015
|   |   |   |   |__ 📁 10
|   |   |   |   |   |__ 📁 01
|   |   |   |   |   |   |__ 📁 01
|   |   |   |   |   |   |   |__ 📃 000000.added.nt
|   |   |   |   |   |   |   |__ 📃 000000.removed.nt
|   |   |   |   |   |   |   |__ ...
|   |   |   |   |   |   |__ ...
|   |   |   |   |   |__ ...
```

### Generate queries
Note: this method is massively overkill for just replicating the changelogs exactly
```shell
# compress dataset (this step will take long, and use a lot of RAM (< 128GB))
sparql-update-data-generator compress -D -o dbpedia-dataset.compressor_state dbpedia_2015-10_en_wo-comments_c.nt

# compress diff n-triples files (this stop will take a little while)
sparql-update-data-generator compress -i dbpedia-dataset.compressor_state -o dbpedia-dataset-and-queries.compressor_state -r downloads.dbpedia.org

# replicate diff n-triples files as queries (this step will be very fast)
sparql-update-data-generator replicate -r -o test-queries.txt -s dbpedia-dataset-and-queries.compressor_state downloads.dbpedia.org
```
