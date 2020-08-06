--fx_ngrams.sql
--Snowflake Javascript function to create ngrams from string

CREATE OR REPLACE FUNCTION fx_ngrams (str varchar, length double)
RETURNS array
LANGUAGE JAVASCRIPT
---execute as caller
AS
$$

    var ngramsArray = [];
    var str = STR;
    var length = LENGTH;

    //clean up junk
    str = str.replace(/[^A-Za-z0-9_]/g," ");

    //clean up double spaces
    str = str.replace(/ {1,}/g," ");

    var array_from_str = str.split(" ");

    for (var i = 0; i < array_from_str.length - (length - 1); i++) {
        var subNgramsArray = [];

        for (var j = 0; j < length; j++) {
            subNgramsArray.push(array_from_str[i + j])
        }

        ngramsArray.push(subNgramsArray);
    }

    return ngramsArray;

$$
;


SELECT fx_ngrams('This is the best thing ever, Hello World', 5);
select value from table(flatten(fx_npairs('This is the best thing ever, Hello World', 5)));
