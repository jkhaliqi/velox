================
String Functions
================

.. note::

    Unless specified otherwise, all functions return NULL if at least one of the arguments is NULL.

    These functions assume that input strings contain valid UTF-8 encoded Unicode code points.
    The behavior is undefined if they are not.

.. spark:function:: ascii(string) -> integer

    Returns unicode code point of the first character of ``string``. Returns 0 if ``string`` is empty.

.. spark:function:: base64(expr) -> varchar

    Converts ``expr`` to a base 64 string using RFC2045 Base64 transfer encoding for MIME. ::

        SELECT base64('Spark SQL'); -- 'U3BhcmsgU1FM'

.. spark:function:: bit_length(string/binary) -> integer

    Returns the bit length for the specified string column. ::

        SELECT bit_length('123'); -- 24

.. spark:function:: char_type_write_side_check(string, limit) -> varchar

    Ensures that input ``string`` fits within the specified length ``limit`` in characters by padding or trimming spaces as needed.
    If the length of ``string`` is less than ``limit``, it is padded with trailing spaces (ASCII 32) to reach ``limit``.
    If the length of ``string`` is greater than ``limit``, trailing spaces are trimmed to fit within ``limit``.
    Throws exception when ``string`` still exceeds ``limit`` after trimming trailing spaces or when ``limit`` is not greater than 0.
    Note: This function is not directly callable in Spark SQL, but internally used for length check when writing char type columns. ::

        -- Function call examples (this function is not directly callable in Spark SQL).
        char_type_write_side_check("abc", 3) -- "abc"
        char_type_write_side_check("ab", 3) -- "ab "
        char_type_write_side_check("a", 3) -- "a  "
        char_type_write_side_check("abc  ", 3) -- "abc"
        char_type_write_side_check("abcd", 3) -- VeloxUserError: "Exceeds allowed length limitation: '3'"
        char_type_write_side_check("世界", 2) -- "世界"
        char_type_write_side_check("世", 3) -- "世  "
        char_type_write_side_check("世界人", 2) -- VeloxUserError: "Exceeds allowed length limitation: '2'"
        char_type_write_side_check("abc", 0) -- VeloxUserError: "The length limit must be greater than 0."

.. spark:function:: chr(n) -> varchar

    Returns the Unicode code point ``n`` as a single character string.
    If ``n < 0``, the result is an empty string.
    If ``n >= 256``, the result is equivalent to chr(``n % 256``).

.. spark:function:: concat_ws(separator, [string/array<string>], ...) -> varchar

   Returns the concatenation result for ``string`` and all elements in ``array<string>``, separated
   by ``separator``. The first argument is ``separator`` whose type is VARCHAR. Then, this function
   can take variable number of remaining arguments , and it allows mixed use of ``string`` type and
   ``array<string>`` type. Skips NULL argument or NULL array element during the concatenation. If
   ``separator`` is NULL, returns NULL, regardless of the following inputs. For non-NULL ``separator``,
   if no remaining input exists or all remaining inputs are NULL, returns an empty string. ::

        SELECT concat_ws('~', 'a', 'b', 'c'); -- 'a~b~c'
        SELECT concat_ws('~', ['a', 'b', 'c'], ['d']); -- 'a~b~c~d'
        SELECT concat_ws('~', 'a', ['b', 'c']); -- 'a~b~c'
        SELECT concat_ws('~', '', [''], ['a', '']); -- '~~a~'
        SELECT concat_ws(NULL, 'a'); -- NULL
        SELECT concat_ws('~'); -- ''
        SELECT concat_ws('~', NULL, [NULL], 'a', 'b'); -- 'a~b'
        SELECT concat_ws('~', NULL, NULL); -- ''
        SELECT concat_ws('~', [NULL]); -- ''

.. spark:function:: contains(left, right) -> boolean

    Returns true if 'right' is found in 'left'. Otherwise, returns false. ::

        SELECT contains('Spark SQL', 'Spark'); -- true
        SELECT contains('Spark SQL', 'SPARK'); -- false
        SELECT contains('Spark SQL', null); -- NULL
        SELECT contains(x'537061726b2053514c', x'537061726b'); -- true

.. spark:function:: conv(number, fromBase, toBase) -> varchar

    Converts ``number`` represented as a string from ``fromBase`` to ``toBase``.
    ``fromBase`` must be an INTEGER value between 2 and 36 inclusively. ``toBase`` must
    be an INTEGER value between 2 and 36 inclusively or between -36 and -2 inclusively.
    Otherwise, returns NULL.
    Returns a signed number if ``toBase`` is negative. Otherwise, returns an unsigned one.
    Returns NULL if ``number`` is empty.
    Skips leading spaces. ``number`` may contain other characters not valid for ``fromBase``.
    All characters starting from the first invalid character till the end of the string are
    ignored. Only converts valid characters even though ``fromBase`` = ``toBase``. Returns
    '0' if no valid character is found. ::

        SELECT conv('100', 2, 10); -- '4'
        SELECT conv('-10', 16, -10); -- '-16'
        SELECT conv("-1", 10, 16); -- 'FFFFFFFFFFFFFFFF'
        SELECT conv("123", 10, 39); -- NULL
        SELECT conv('', 16, 10); -- NULL
        SELECT conv(' ', 2, 10); -- NULL
        SELECT conv("11", 10, 16); -- 'B'
        SELECT conv("11ABC", 10, 16); -- 'B'
        SELECT conv("11abc", 10, 10); -- '11'
        SELECT conv('H016F', 16, 10); -- '0'

.. spark:function:: empty2null(input) -> varchar

    Returns NULL if ``input`` is empty. Otherwise, returns ``input``.
    Note: it's an internal Spark function used to convert empty value of a partition column,
    which is then converted to Hive default partition value ``__HIVE_DEFAULT_PARTITION__``. ::

        SELECT empty2null(''); -- NULL
        SELECT empty2null('abc'); -- 'abc'

.. spark:function:: endswith(left, right) -> boolean

    Returns true if 'left' ends with 'right'. Otherwise, returns false. ::

        SELECT endswith('js SQL', 'SQL'); -- true
        SELECT endswith('js SQL', 'js'); -- false
        SELECT endswith('js SQL', NULL); -- NULL

.. spark:function:: find_in_set(str, strArray) -> integer

    Returns 1-based index of the given string ``str`` in the comma-delimited list ``strArray``.
    Returns 0, if the string was not found or if the given string ``str`` contains a comma. ::

        SELECT find_in_set('ab', 'abc,b,ab,c,def'); -- 3
        SELECT find_in_set('ab,', 'abc,b,ab,c,def'); -- 0
        SELECT find_in_set('dfg', 'abc,b,ab,c,def'); -- 0
        SELECT find_in_set('', ''); -- 1
        SELECT find_in_set('', '123,'); -- 2
        SELECT find_in_set('', ',123'); -- 1
        SELECT find_in_set(NULL, ',123'); -- NULL
        SELECT find_in_set("abc", NULL); -- NULL

.. spark:function:: initcap(string) -> varchar

   The ``initcap`` function converts the first character of each word to uppercase
   and all other characters in the word to lowercase. It supports UTF-8 multibyte
   characters, up to four bytes per character.

   A *word* is defined as a sequence of characters separated by whitespace. ::

        SELECT initcap('spark sql'); -- Spark Sql
        SELECT initcap('spARK sQL'); -- Spark Sql
        SELECT initcap('123abc DEF!ghi'); -- 123abc Def!ghi
        SELECT initcap('élan vital für alle'); -- Élan Vital Für Alle
        SELECT initcap('hello-world test_case'); -- Hello-world Test_case

.. spark:function:: instr(string, substring) -> integer

    Returns the starting position of the first instance of ``substring`` in
    ``string``. Positions start with ``1``. Returns 0 if 'substring' is not found.

.. spark:function:: left(string, length) -> string

    Returns the leftmost length characters from the ``string``.
    If ``length`` is less or equal than 0 the result is an empty string.

.. spark:function:: length(string) -> integer

    Returns the length of ``string`` in characters.

.. spark:function:: levenshtein(string1, string2[, threshold]) -> integer

    Returns the `Levenshtein distance <https://en.wikipedia.org/wiki/Levenshtein_distance>`_ between the two given strings.
    If the provided ``threshold`` is negative, or the levenshtein distance exceeds ``threshold``, returns -1. ::

        SELECT levenshtein('kitten', 'sitting'); -- 3
        SELECT levenshtein('kitten', 'sitting', 10); -- 3
        SELECT levenshtein('kitten', 'sitting', 2); -- -1

.. spark:function:: locate(substring, string, start) -> integer

    Returns the 1-based position of the first occurrence of ``substring`` in given ``string``
    after position ``start``. The search is from the beginning of ``string`` to the end.
    ``start`` is the starting character position in ``string`` to search for the ``substring``.
    ``start`` is 1-based and must be at least 1 and at most the characters number of ``string``.
    The following rules on special values are applied to follow Spark's implementation.
    They are listed in order of priority:

    Returns 0 if ``start`` is NULL. Returns NULL if ``substring`` or ``string`` is NULL.
    Returns 0 if ``start`` is less than 1.
    Returns 1 if ``substring`` is empty.
    Returns 0 if ``start`` is greater than the characters number of ``string``.
    Returns 0 if ``substring`` is not found in ``string``. ::

        SELECT locate('aa', 'aaads', 1); -- 1
        SELECT locate('aa', 'aaads', -1); -- 0
        SELECT locate('aa', 'aaads', 2); -- 2
        SELECT locate('aa', 'aaads', 6); -- 0
        SELECT locate('aa', 'aaads', NULL); -- 0
        SELECT locate('', 'aaads', 1); -- 1
        SELECT locate('', 'aaads', 9); -- 1
        SELECT locate('', 'aaads', -1); -- 0
        SELECT locate('', '', 1); -- 1
        SELECT locate('aa', '', 1); -- 0
        SELECT locate(NULL, NULL, NULL); -- 0
        SELECT locate(NULL, NULL, 1); -- NULL
        SELECT locate('\u4FE1', '\u4FE1\u5FF5,\u4FE1\u7231,\u4FE1\u5E0C\u671B', 2); -- 4

.. spark:function:: lower(string) -> string

    Returns string with all characters changed to lowercase. ::

        SELECT lower('SparkSql'); -- sparksql

.. spark:function:: lpad(string, len, pad) -> string

    Returns ``string``, left-padded with pad to a length of ``len``. If ``string`` is
    longer than ``len``, the return value is shortened to ``len`` characters or bytes.
    If ``pad`` is not specified, ``string`` will be padded to the left with space characters
    if it is a character string, and with zeros if it is a byte sequence. ::

        SELECT lpad('hi', 5, '??'); -- ???hi
        SELECT lpad('hi', 1, '??'); -- h
        SELECT lpad('hi', 4); --   hi

.. spark:function:: ltrim(string) -> varchar

    Removes leading 0x20(space) characters from ``string``. ::

        SELECT ltrim('  data  '); -- "data  "

.. spark:function:: ltrim(trimCharacters, string) -> varchar
    :noindex:

    Removes specified leading characters from ``string``. The specified character
    is any character contained in ``trimCharacters``.
    ``trimCharacters`` can be empty and may contain duplicate characters. ::

        SELECT ltrim('ps', 'spark'); -- "ark"

.. spark:function:: luhn_check(string) -> boolean

    Returns true if ``string`` passes the Luhn algorithm check. Otherwise, returns false.
    The Luhn algorithm is a simple check digit formula used to validate a variety of identification numbers,
    defined in US patent 2950048A.
    Returns NULL if ``string`` is NULL. ::

        SELECT luhn_check('4111111111111111'); -- true
        SELECT luhn_check('378282246310006'); -- false
        SELECT luhn_check(NULL); -- NULL

.. spark:function:: mask(string[, upperChar, lowerChar, digitChar, otherChar]) -> string

    Returns a masked version of the input ``string``.
    ``string``: string value to mask.
    ``upperChar``: A single character string used to substitute upper case characters. The default is 'X'. If NULL, upper case characters remain unmasked.
    ``lowerChar``: A single character string used to substitute lower case characters. The default is 'x'. If NULL, lower case characters remain unmasked.
    ``digitChar``: A single character string used to substitute digits. The default is 'n'. If NULL, digits remain unmasked.
    ``otherChar``: A single character string used to substitute any other character. The default is NULL, which leaves these characters unmasked.
    Any invalid UTF-8 characters present in the input string will be treated as a single other character. ::

        SELECT mask('abcd-EFGH-8765-4321');  -- "xxxx-XXXX-nnnn-nnnn"
        SELECT mask('abcd-EFGH-8765-4321', 'Q');  -- "xxxx-QQQQ-nnnn-nnnn"
        SELECT mask('AbCD123-@$#');  -- "XxXXnnn-@$#"
        SELECT mask('AbCD123-@$#', 'Q');  -- "QxQQnnn-@$#"
        SELECT mask('AbCD123-@$#', 'Q', 'q');  -- "QqQQnnn-@$#"
        SELECT mask('AbCD123-@$#', 'Q', 'q', 'd');  -- "QqQQddd-@$#"
        SELECT mask('AbCD123-@$#', 'Q', 'q', 'd', 'o');  -- "QqQQdddoooo"
        SELECT mask('AbCD123-@$#', NULL, 'q', 'd', 'o'); -- "AqCDdddoooo"
        SELECT mask('AbCD123-@$#', NULL, NULL, 'd', 'o'); -- "AbCDdddoooo"
        SELECT mask('AbCD123-@$#', NULL, NULL, NULL, 'o'); -- "AbCD123oooo"
        SELECT mask(NULL, NULL, NULL, NULL, 'o'); -- NULL
        SELECT mask(NULL); -- NULL
        SELECT mask('AbCD123-@$#', NULL, NULL, NULL, NULL); -- "AbCD123-@$#"

.. spark:function:: overlay(input, replace, pos, len) -> same as input

    Replace a substring of ``input`` starting at ``pos`` character with ``replace`` and
    going for rest ``len`` characters of ``input``.
    Types of ``input`` and ``replace`` must be the same. Supported types are: VARCHAR and VARBINARY.
    When ``input`` types are VARCHAR, ``len`` and ``pos`` are specified in characters, otherwise, bytes.
    Result is constructed from three parts.
    First part is first pos - 1 characters of ``input`` when ``pos`` if greater then zero, otherwise, empty string.
    Second part is ``replace``.
    Third part is rest of ``input`` from indices pos + len which is 1-based,
    if ``len`` is negative, it will be set to size of ``replace``,
    if pos + len is negative, it refers to -(pos + len)th element before the end of ``input``.
    ::

        SELECT overlay('Spark SQL', '_', 6, -1); -- "Spark_SQL"
        SELECT overlay('Spark SQL', 'CORE', 7, -1); -- "Spark CORE"
        SELECT overlay('Spark SQL', 'ANSI ', 7, 0); -- "Spark ANSI SQL"
        SELECT overlay('Spark SQL', 'tructured', 2, 4); -- "Structured SQL"
        SELECT overlay('Spark SQL', '_', -6, 3); -- "_Sql"

.. spark:function:: repeat(input, n) -> varchar

    Returns the string which repeats ``input`` ``n`` times.
    Result size must be less than or equal to 1MB.
    If ``n`` is less than or equal to 0, empty string is returned. ::

        SELECT repeat('123', 2); -- 123123

.. spark:function:: replace(input, replaced) -> varchar

    Removes all instances of ``replaced`` from ``input``.
    If ``replaced`` is an empty string, returns the original ``input`` string. ::

        SELECT replace('ABCabc', ''); -- ABCabc
        SELECT replace('ABCabc', 'bc'); -- ABCc

.. spark:function:: replace(input, replaced, replacement) -> varchar

    Replaces all instances of ``replaced`` with ``replacement`` in ``input``.
    If ``replaced`` is an empty string, returns the original ``input`` string. ::

        SELECT replace('ABCabc', '', 'DEF'); -- ABCabc
        SELECT replace('ABCabc', 'abc', ''); -- ABC
        SELECT replace('ABCabc', 'abc', 'DEF'); -- ABCDEF

.. spark:function:: reverse(string) -> varchar

    Returns input string with characters in reverse order.

.. spark:function:: rpad(string, len, pad) -> string

    Returns ``string``, right-padded with ``pad`` to a length of ``len``.
    If ``string`` is longer than ``len``, the return value is shortened to ``len`` characters.
    If ``pad`` is not specified, ``string`` will be padded to the right with space characters
    if it is a character string, and with zeros if it is a binary string. ::

        SELECT lpad('hi', 5, '??'); -- ???hi
        SELECT lpad('hi', 1, '??'); -- h
        SELECT lpad('hi', 4); -- hi

.. spark:function:: rtrim(string) -> varchar

    Removes trailing 0x20(space) characters from ``string``. ::

        SELECT rtrim('  data  '); -- "  data"

.. spark:function:: rtrim(trimCharacters, string) -> varchar
    :noindex:

    Removes specified trailing characters from ``string``. The specified character
    is any character contained in ``trimCharacters``.
    ``trimCharacters`` can be empty and may contain duplicate characters. ::

        SELECT rtrim('kr', 'spark'); -- "spa"

.. spark:function:: soundex(string) -> string

    Returns `Soundex code <https://en.wikipedia.org/wiki/Soundex>`_ of the string. If first character of ``string`` is not
    a letter, ``string`` is returned. ::

        SELECT soundex('Miller'); -- "M460"

.. spark:function:: split(string, delimiter[, limit]) -> array(string)

    Splits ``string`` around occurrences that match ``delimiter`` and returns an array with a length of
    at most ``limit``. ``delimiter`` is a string representing regular expression. ``limit`` is an integer
    which controls the number of times the regex is applied. By default, ``limit`` is -1. When ``limit`` > 0,
    the resulting array's length will not be more than ``limit``, and the resulting array's last entry will
    contain all input beyond the last matched regex. When ``limit`` <= 0, ``regex`` will be applied as many
    times as possible, and the resulting array can be of any size. When ``delimiter`` is empty, if ``limit``
    is smaller than the size of ``string``, the resulting array only contains ``limit`` number of single characters
    splitting from ``string``, if ``limit`` is not provided or is larger than the size of ``string``, the resulting
    array contains all the single characters of ``string`` and does not include an empty tail character.
    The split function align with vanilla spark 3.4+ split function. ::

        SELECT split('oneAtwoBthreeC', '[ABC]'); -- ["one","two","three",""]
        SELECT split('oneAtwoBthreeC', '[ABC]', 2); -- ["one","twoBthreeC"]
        SELECT split('oneAtwoBthreeC', '[ABC]', 5); -- ["one","two","three",""]
        SELECT split('one', '1'); -- ["one"]
        SELECT split('abcd', ''); -- ["a","b","c","d"]
        SELECT split('abcd', '', 3); -- ["a","b","c"]
        SELECT split('abcd', '', 5); -- ["a","b","c","d"]

.. spark:function:: startswith(left, right) -> boolean

    Returns true if 'left' starts with 'right'. Otherwise, returns false. ::

        SELECT startswith('js SQL', 'js'); -- true
        SELECT startswith('js SQL', 'SQL'); -- false
        SELECT startswith('js SQL', null); -- NULL

.. spark:function:: str_to_map(string, entryDelimiter, keyValueDelimiter) -> map(string, string)

    Returns a map by splitting ``string`` into entries with ``entryDelimiter`` and splitting
    each entry into key/value with ``keyValueDelimiter``.
    ``entryDelimiter`` and ``keyValueDelimiter`` must be constant strings with single ascii
    character. Allows ``keyValueDelimiter`` not found when splitting an entry. Throws exception
    when duplicate map keys are found for single row's result, consistent with Spark's default
    behavior. ::

        SELECT str_to_map('a:1,b:2,c:3', ',', ':'); -- {"a":"1","b":"2","c":"3"}
        SELECT str_to_map('a', ',', ':'); -- {"a":NULL}
        SELECT str_to_map('', ',', ':'); -- {"":NULL}
        SELECT str_to_map('a:1,b:2,c:3', ',', ','); -- {"a:1":NULL,"b:2":NULL,"c:3":NULL}

.. spark:function:: substring(string, start) -> varchar

    Returns the rest of ``string`` from the starting position ``start``.
    Positions start with ``1``. A negative starting position is interpreted
    as being relative to the end of the string. When the starting position is 0,
    the meaning is to refer to the first character.Type of 'start' must be an INTEGER.

.. spark:function:: substring(string, start, length) -> varchar
    :noindex:

    Returns a substring from ``string`` of length ``length`` from the starting
    position ``start``. Positions start with ``1``. A negative starting
    position is interpreted as being relative to the end of the string.
    When the starting position is 0, the meaning is to refer to the first character.
    Type of 'start' must be an INTEGER. ::

        SELECT substring('Spark SQL', 0, 2); -- Sp
        SELECT substring('Spark SQL', 1, 2); -- Sp
        SELECT substring('Spark SQL', 5, 0); -- ""
        SELECT substring('Spark SQL', 5, -1); -- ""
        SELECT substring('Spark SQL', 5, 10000); -- "k SQL"
        SELECT substring('Spark SQL', -9, 3); -- "Spa"
        SELECT substring('Spark SQL', -10, 3); -- "Sp"
        SELECT substring('Spark SQL', -20, 3); -- ""

.. spark:function:: substring_index(string, delim, count) -> [same as string]

    Returns the substring from ``string`` before ``count`` occurrences of the delimiter ``delim``.
    Here the ``string`` can be VARCHAR or VARBINARY and return type matches type of ``string``.
    If ``count`` is positive, returns everything to the left of the final delimiter
    (counting from the left). If ``count`` is negative, returns everything to the right
    of the final delimiter (counting from the right). If ``count`` is 0, returns empty string.
    If ``delim`` is not found or found fewer times than ``count``, returns the original input string.
    ``delim`` is case-sensitive. It also takes into account overlapping strings. ::

        SELECT substring_index('Spark.SQL', '.', 1); -- "Spark"
        SELECT substring_index('Spark.SQL', '.', 0); -- ""
        SELECT substring_index('Spark.SQL', '.', -1); -- "SQL"
        SELECT substring_index('TEST.Spark.SQL', '.',2); -- "TEST.Spark"
        SELECT substring_index('TEST.Spark.SQL', '', 0); -- ""
        SELECT substring_index('TEST.Spark.SQL', '.', -2); -- "Spark.SQL"
        SELECT substring_index('TEST.Spark.SQL', '.', 10); -- "TEST.Spark.SQL"
        SELECT substring_index('TEST.Spark.SQL', '.', -12); -- "TEST.Spark.SQL"
        SELECT substring_index('aaaaa', 'aa', 2); -- "a"
        SELECT substring_index('aaaaa', 'aa', -4); -- "aaa"
        SELECT substring_index('aaaaa', 'aa', 0); -- ""
        SELECT substring_index('aaaaa', 'aa', 5); -- "aaaaa"
        SELECT substring_index('aaaaa', 'aa', -5); -- "aaaaa"

.. spark:function:: translate(string, match, replace) -> varchar

    Returns a new translated string. It translates the character in ``string`` by a
    character in ``replace``. The character in ``replace`` is corresponding to
    the character in ``match``. The translation will happen when any character
    in ``string`` matching with a character in ``match``. If ``match's`` character
    size is larger than ``replace's``, the extra characters in ``match`` will be
    removed from ``string``. In addition, this function only considers the first
    occurrence of a character in ``match`` and uses its corresponding character in
    ``replace`` for translation.
    Any invalid UTF-8 characters present in the input string will be treated as a
    single character.::

        SELECT translate('spark', 'sa', '12');  -- "1p2rk"
        SELECT translate('spark', 'sa', '1');   -- "1prk"
        SELECT translate('spark', 'ss', '12');  -- "1park"

.. spark:function:: trim(string) -> varchar

    Removes leading and trailing 0x20(space) characters from ``string``. ::

        SELECT trim('  data  '); -- "data"

.. spark:function:: trim(trimCharacters, string) -> varchar
    :noindex:

    Removes specified leading and trailing characters from ``string``.
    The specified character is any character contained in ``trimCharacters``.
    ``trimCharacters`` can be empty and may contain duplicate characters. ::

        SELECT trim('sprk', 'spark'); -- "a"

.. spark:function:: unbase64(expr) -> varbinary

    Returns a decoded base64 string as binary. ::

        SELECT cast(unbase64('U3BhcmsgU1FM') AS STRING); -- 'Spark SQL'

.. spark:function:: upper(string) -> string

    Returns string with all characters changed to uppercase. ::

        SELECT upper('SparkSql'); -- SPARKSQL

.. spark:function:: varchar_type_write_side_check(string, limit) -> varchar

    Removes trailing space characters (ASCII 32) that exceed the length ``limit`` from the end of input ``string``. ``limit`` is the maximum length of characters that can be allowed.
    Throws exception when ``string`` still exceeds ``limit`` after trimming trailing spaces or when ``limit`` is not greater than 0.
    Empty strings are returned as-is since they always satisfy any length ``limit`` greater than 0.
    Note: This function is not directly callable in Spark SQL, but internally used for length check when writing string type columns. ::

        -- Function call examples (this function is not directly callable in Spark SQL).
        varchar_type_write_side_check("abc", 3) -- "abc"
        varchar_type_write_side_check("abc   ", 3) -- "abc"
        varchar_type_write_side_check("abcd", 3) -- VeloxUserError: "Exceeds allowed length limitation: '3'"
        varchar_type_write_side_check("中国", 3) -- "中国"
        varchar_type_write_side_check("中文中国", 3) -- VeloxUserError: "Exceeds allowed length limitation: '3'"
        varchar_type_write_side_check("   ", 0) -- VeloxUserError: "The length limit must be greater than 0."
        varchar_type_write_side_check("", 3) -- ""
