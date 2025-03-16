import json
import boto3

def lambda_handler(event, context):
    job_name = 'glue-job-spec'

    # Inicializa o cliente do Glue
    glue_client = boto3.client('glue')

    try:
        response = glue_client.start_job_run(JobName=job_name)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Job do Glue iniciado com sucesso!'),
            'jobRunId': response['JobRunId']
        }

    except Exception as e:
        # Retorna erro caso algo dê errado
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro ao iniciar o job do Glue: {str(e)}')
        }