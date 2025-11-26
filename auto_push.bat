@echo off

REM ===== 设置仓库路径 =====
cd /d D:\Desktop\bond-etf-auto

echo Checking input updates...

REM ===== 添加 input 文件 =====
git add input/

REM ===== 生成带日期的提交信息 =====
set DATE=%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%
git commit -m "update input %DATE%" || echo No changes to commit

REM ===== 推送到 GitHub =====
git push

echo Done.
pause
