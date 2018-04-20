**CSV Reader on Mist 1.0.0-RC14**

make run-mist
make test package
make deploy-csv

Mist UI localhost:2004/ui

```json
{
	"pathToCsv": "resources/Sample3.csv",
	"mutators": [
		"{existing_col_name : name, new_col_name: track name, new_data_type: string}",
		"{existing_col_name : n, new_col_name : number in album, new_data_type : integer}",
		"{existing_col_name : time, new_col_name : time, new_data_type : string}"
	]
}
```

curl -X POST -d '{"pathToCsv":"resources/Sample3.csv","mutators":["{existing_col_name : name, new_col_name: track name, new_data_type: string}","{existing_col_name : n, new_col_name : number in album, new_data_type : integer}","{existing_col_name : time, new_col_name : time, new_data_type : string}"]}' 'http://localhost:2004/v2/api/functions/dev_csv-reader/jobs'
