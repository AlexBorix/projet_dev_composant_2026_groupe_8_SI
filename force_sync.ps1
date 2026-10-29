$ErrorActionPreference = 'Stop'
$body = @{client_id='0b408f4f-27d5-4172-a129-ba834df4c14d'; client_secret='QSCGpsc7WYuJUF31meZAL7FPNWAvaq0C'} | ConvertTo-Json
$token = (Invoke-RestMethod -Uri 'http://localhost:8000/api/public/v1/applications/token' -Method Post -Body $body -ContentType 'application/json').access_token
$jobBody = @{connectionId='a3598384-448e-42ce-8a9f-519f3f1a47e1'; jobType='sync'} | ConvertTo-Json
$job = Invoke-RestMethod -Uri 'http://localhost:8000/api/public/v1/jobs' -Method Post -Headers @{Authorization="Bearer $token"} -Body $jobBody -ContentType 'application/json'
$jobId = $job.jobId
Write-Host "Job: $jobId"
