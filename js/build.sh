#cat `cat manifest.js.complete.txt` | closure > hailwhale.js.partial.js
cat `cat manifest.coffee.complete.txt` | coffee -c -s | closure > hailwhale.coffee.partial.js
rm hailwhale.complete.js
cat hailwhale.{js,coffee}.partial.js >> hailwhale.complete.js
