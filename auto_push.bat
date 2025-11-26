@echo off
cd /d %~dp0

set DATE=%date:~0,4%-%date:~5,2%-%date:~8,2%

echo =====================================
echo 🔁 自动提交并推送 (%DATE%) ...
echo =====================================

git add .

git commit -m "update on %DATE%" 2>nul

git push

echo =====================================
echo ✅ 已成功推送！
echo =====================================

pause
