from google.cloud import bigquery
from google.cloud.bigquery import Dataset
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

# credentials to list project
credentials = GoogleCredentials.get_application_default()
service = discovery.build('cloudresourcemanager', 'v1', credentials=credentials)

text_file = open("bigquerySizeGB.csv", "w")

# list project
request = service.projects().list()
response = request.execute()

print('inicio -------------------')

n = text_file.write('project_name, dataset_name, table_name, size_GB\n')

# Main loop for project
for project in response.get('projects', []):
    project_name = project['name']
    client = bigquery.Client(project['projectId']) # Start the client in the right project
    
    # list dataset
    datasets = []
    try:
        datasets = list(client.list_datasets())

        if datasets: # If there is some BQ dataset
            # print('Datasets in project {}:'.format(project['name']))
            # Second loop to list the tables in the dataset
            for dataset in datasets: 
                dataset_name = dataset.dataset_id
                # print(' - {}'.format(dataset.dataset_id))
                
                get_size = client.query("select table_id, size_bytes/(1024*1024*1024) as size_GB from "+dataset.dataset_id+".__TABLES__") # This query retrieve all the tables in the dataset and the size in bytes. It can be modified to get more fields.
                tables = get_size.result() # Get the result

                # Third loop to list the tables and print the result
                for table in tables:
                    table_name = table.table_id
                    # print('\t{} size: {}'.format(table.table_id,table.size))
                    # print('{}, {}, {}, {}'.format(project_name, dataset_name, table_name, table.size_GB))
                    n = text_file.write('{}, {}, {}, {}'.format(project_name, dataset_name, table_name, table.size_GB))
                    n = text_file.write('\n')
    except Exception as e:
        print(e)
        pass

text_file.close()
print('fim -------------------')
