$ErrorActionPreference = 'Stop'
$body = @{client_id='0b408f4f-27d5-4172-a129-ba834df4c14d'; client_secret='QSCGpsc7WYuJUF31meZAL7FPNWAvaq0C'} | ConvertTo-Json
$token = (Invoke-RestMethod -Uri 'http://localhost:8000/api/public/v1/applications/token' -Method Post -Body $body -ContentType 'application/json').access_token
for ($i=0; $i -lt 60; $i++) {
  $status = (Invoke-RestMethod -Uri 'http://localhost:8000/api/public/v1/jobs/7' -Headers @{Authorization="Bearer $token"}).status
  Write-Host "Status: $status"
  if ($status -eq 'succeeded') { break }
  if ($status -ne 'pending' -and $status -ne 'running') { throw "Failed: $status" }
  Start-Sleep -Seconds 10
}
