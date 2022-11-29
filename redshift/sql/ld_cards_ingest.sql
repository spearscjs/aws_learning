
--Loading datasets from s3 to redshift


--Loading datasets from s3 to redshift for 2022-01-02
copy cards_ingest.account_src
from 's3://aws-train-nov-de/cards_ingest/account_src/cards_account_ingest_2022-01-02.csv'
iam_role 'arn:aws:iam::341966982503:role/admin_redshift_access'
csv
IGNOREHEADER 1;

--Loading datasets from s3 to redshift for 2022-01-03
copy cards_ingest.account_src
from 's3://aws-train-nov-de/cards_ingest/account_src/cards_account_ingest_2022-01-03.csv'
iam_role 'arn:aws:iam::341966982503:role/admin_redshift_access'
csv
IGNOREHEADER 1;
