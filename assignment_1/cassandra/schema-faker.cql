USE kafkapipeline;
CREATE TABLE IF NOT EXISTS kafkapipeline.fakerdata (
  name TEXT,
  address TEXT,
  year INT,
  phone TEXT,
  credit_card TEXT,
  company_name TEXT,
  city TEXT,
  job TEXT,
  color TEXT,
  country TEXT,
  PRIMARY KEY (name, address)
);