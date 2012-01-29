cat `cat manifest.js.complete.txt` > hailwhale.js.partial.js
cat `cat manifest.coffee.complete.txt` | coffee -c -s > hailwhale.coffee.partial.js
rm -f hailwhale.complete.js
rm -f hailwhale.dev_includes.html
cat hailwhale.{js,coffee}.partial.js >> hailwhale.complete.js
for i in `cat manifest.js.complete.txt`; do
    echo "<script src='/js/$i'></script>">>hailwhale.dev_includes.html
done;
