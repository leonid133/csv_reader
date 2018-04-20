[![Build Status](https://travis-ci.org/leonid133/csv_reader.svg?branch=master)](https://travis-ci.org/leonid133/csv_reader)

## CSV Reader on [Hydrosphere Mist 1.0.0-RC14](https://hydrosphere.io) Spark 2.2.0

make run-mist
make test package
make deploy-csv

Mist UI localhost:2004/ui

Input Parameters:
```json
{
  "pathToCsv": "tmp/Sample3.csv",
  "mutators": [
    "{existing_col_name : name, new_col_name: new name, new_data_type: string}",
    "{existing_col_name : age, new_col_name : total years, new_data_type : integer}",
    "{existing_col_name : birthday, new_col_name : d_o_b, new_data_type : date, date_expression : dd-MM-yyyy}"
  ]
}
```

or

```sh
curl -X POST -d '{"pathToCsv":"tmp/Sample3.csv","mutators":["{existing_col_name : name, new_col_name: new name, new_data_type: string}","{existing_col_name : age, new_col_name : total years, new_data_type : integer}","{existing_col_name : birthday, new_col_name : d_o_b, new_data_type : date, date_expression : dd-MM-yyyy}"]}' 'http://localhost:2004/v2/api/functions/dev_csv-reader/jobs'
```