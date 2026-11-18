[CmdletBinding()]
param(
    [ValidateSet('demo', 'raw')]
    [string]$Source = 'demo',
    [int]$TimeoutMinutes = 30,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) {
        throw "Docker exited with code $LASTEXITCODE."
    }
}

if ($Source -eq 'raw') {
    $config = Invoke-Docker compose config --format json | ConvertFrom-Json
    $dataMount = $config.services.'airflow-scheduler'.volumes |
        Where-Object target -EQ '/pipeline/data/raw'
    if (-not (Get-ChildItem -LiteralPath $dataMount.source -File -Filter '*.csv' -ErrorAction SilentlyContinue)) {
        throw "Extract the Kaggle CSVs directly into $($dataMount.source) before running -Source raw."
    }
}

Invoke-Docker version --format '{{.Server.Version}}'
if ($SkipBuild) {
    Invoke-Docker compose up -d --no-build
} else {
    Invoke-Docker compose up -d --build
}
$dag = 'steam_01_ingest_to_lake'
$runId = "local_$([Guid]::NewGuid().ToString('N'))"
$deadline = (Get-Date).AddMinutes($TimeoutMinutes)

do {
    $registered = Invoke-Docker compose exec -T postgres psql -U steam -d airflow -At -c "SELECT count(*) FROM serialized_dag WHERE dag_id IN ('$dag', 'steam_02_lake_to_warehouse');"
    if ($registered -eq '2') {
        break
    }
    Start-Sleep -Seconds 5
} while ((Get-Date) -lt $deadline)
if ($registered -ne '2') {
    throw 'Airflow did not register both DAGs. Run docker compose logs airflow-scheduler.'
}

Invoke-Docker compose exec -T airflow-scheduler airflow dags unpause steam_02_lake_to_warehouse
Invoke-Docker compose exec -T airflow-scheduler airflow dags unpause $dag
$triggerCode = 'import json,sys; from airflow.__main__ import main; dag,run,source=sys.argv[1:]; sys.argv=[sys.argv[0],"dags","trigger",dag,"--run-id",run,"--conf",json.dumps(dict(source=source))]; main()'
$triggerCode | docker compose exec -T airflow-scheduler python - $dag $runId $Source
if ($LASTEXITCODE -ne 0) { throw 'Failed to trigger the Airflow DAG.' }

do {
    Start-Sleep -Seconds 5
    $state = Invoke-Docker compose exec -T postgres psql -U steam -d airflow -At -c "SELECT state FROM dag_run WHERE dag_id = '$dag' AND run_id = '$runId';"
    if ($state -eq 'success') {
        $schema = if ($Source -eq 'demo') { 'analytics_demo' } else { 'analytics' }
        $totals = Invoke-Docker compose exec -T postgres psql -U steam -d steam_reviews -v ON_ERROR_STOP=1 -At -c "SELECT SUM(reviews), SUM(recommendations) FROM $schema.recommendation_trend;"
        if ($Source -eq 'demo' -and $totals -ne '6|4') {
            throw "Demo expected 6 reviews and 4 recommendations, got $totals."
        }
        $reviews, $recommendations = $totals -split '\|'
        Write-Output "Reviews: $reviews. Recommendations: $recommendations."
        return
    }
    if ($state -eq 'failed') {
        throw "Airflow run $runId failed. Open the two DAGs in Airflow to see the task logs."
    }
} while ((Get-Date) -lt $deadline)
throw "Airflow run $runId is still pending or running. Check the Airflow UI."
