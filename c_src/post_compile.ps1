Write-Host "===> -------------------------------------------------------------------------"
Write-Host "===> post_compile ocinif @ $pwd" -foregroundcolor green
Remove-Item c_src\*.o -Force
Remove-Item c_src\*.d -Force
Remove-Item priv\*.exp -Force
Remove-Item priv\*.lib -Force
Copy-Item "$Env:INSTANT_CLIENT_LIB_PATH\*.dll" -Destination ".\priv\"