get_keys = function (obj)
{
  var keys = [];
  for(var i in obj) if (obj.hasOwnProperty(i))
  {
    keys.push(i);
  }
  return keys;
}
