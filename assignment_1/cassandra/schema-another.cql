USE kafkapipeline;
CREATE TABLE IF NOT EXISTS kafkapipeline.anotherdata (
  dna_sequence TEXT,
  rna_sequence TEXT,
  software_license TEXT,
  version TEXT,
  programming_language TEXT,
  os TEXT,
  issn TEXT,
  isbn TEXT,
  ean TEXT,
  pin TEXT,
  PRIMARY KEY (dna_sequence)
);