# Academic Search APIs

*Added in nacsos_data:v0.22.0 (June 2025)*

## Usage

All API wrappers pretty much work the same way as shown here but might have varying parameters.

```bash
# Go to where the library is and activate environment
cd /path/to/repository/root
source /path/to/env/bin/activate

python src/nacsos_data/util/academic/apis/scopus.py \
       download \
       --api-key "??" \
       --query "TITLE-ABS-KEY(\"school uniform\")" \
       --target data/scopus-raw.jsonl
python src/nacsos_data/util/academic/apis/scopus.py \
       convert \
       --source data/scopus-raw.jsonl \
       --target data/scopus-items.jsonl
```

If you installed the package, you also get a convenience method:

```bash
# Activate environment where nacsos_data is installed in
source /path/to/env/bin/activate

# See all options
academic_api --help

# Download from API
academic_api SCOPUS \
       download \
       --api-key "??" \
       --query "TITLE-ABS-KEY(\"school uniform\")" \
       --target data/scopus-raw.jsonl

# Convert to NACSOS AcademicItems
academic_api SCOPUS \
       convert \
       --source "TITLE-ABS-KEY(\"school uniform\")" \
       --target data/scopus-raw.jsonl
```

For larger queries, it might be easier to instead store them in a separate file and change the `--query` parameter to `--query-file="/path/to/file"`.

### Comparison of PubMed, Scopus, Web of Science, and Google Scholar: strengths and weaknesses

*Matthew E Falagas 1, Eleni I Pitsouni, George A Malietzis, Georgios Pappas*
PMID: 17884971 DOI: 10.1096/fj.07-9492LSF
https://pubmed.ncbi.nlm.nih.gov/17884971/
The evolution of the electronic age has led to the development of numerous medical databases on the World Wide Web, offering search
facilities on a particular subject and the ability to perform citation analysis. We compared the content coverage and practical utility of
PubMed, Scopus, Web of Science, and Google Scholar. The official Web pages of the databases were used to extract information on the range of
journals covered, search facilities and restrictions, and update frequency. We used the example of a keyword search to evaluate the
usefulness of these databases in biomedical information retrieval and a specific published article to evaluate their utility in performing
citation analysis. All databases were practical in use and offered numerous search facilities. PubMed and Google Scholar are accessed for
free. The keyword search with PubMed offers optimal update frequency and includes online early articles; other databases can rate articles
by number of citations, as an index of importance. For citation analysis, Scopus offers about 20% more coverage than Web of Science, whereas
Google Scholar offers results of inconsistent accuracy. PubMed remains an optimal tool in biomedical electronic research. Scopus covers a
wider journal range, of help both in keyword searching and citation analysis, but it is currently limited to recent articles (published
after 1995) compared with Web of Science. Google Scholar, as for the Web in general, can help in the retrieval of even the most obscure
information but its use is marred by inadequate, less often updated, citation information.
