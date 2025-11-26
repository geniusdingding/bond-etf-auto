@echo off
echo Adding input files...
git add input
git commit -m "update input" || echo No changes to commit
git push
echo Done.
