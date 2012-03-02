rm -f hailwhale.min.js
for i in `cat manifest.js.complete.txt`; do 
    closure --js $i >> hailwhale.min.js
done;
rm -f hailwhale.dev_includes.html
for i in `cat manifest.js.complete.txt`; do
    echo "<script src='/js/$i'></script>">>hailwhale.dev_includes.html
done;
