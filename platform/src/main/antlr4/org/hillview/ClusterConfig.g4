/**
 * Grammar describing a cluster configuration file.  A subset of Python.
 */
grammar ClusterConfig;

init       : assignment* ;
assignment : IDENTIFIER '=' expression ;
expression : STRING
           | NUMBER
           | BOOL
           | array
           | map
           | expression '+' expression
           | IDENTIFIER
           ;

array      : '[' expression ( ',' expression )* ']' ;
map        : '{' STRING ':' expression ( ',' STRING ':' expression )* '}' ;

// These are from the ANTRL grammar samples for Python
COMMENT    : '#' (~[\n])* [\n] -> skip ;
BOOL       : 'True' | 'False' ;
WS         : [ \r\t\n] -> skip ;
IDENTIFIER :  [a-zA-Z_] [a-zA-Z0-9_]* ;
NUMBER     :   '0' ([xX] [0-9a-fA-F]+         ([lL])?
           |        [oO] [0-7]+                [lL]?
           |        [bB] [01]+                 [lL]?)
           |  [0-9]+                          ([lL])?
           ;
STRING     : ([uUbB]? [rR]? | [rR]? [uUbB]?)
           ( '\''     ('\\' (([ \t]+ ('\r'? '\n')?)|.) | ~[\\\r\n'])*  '\''
           | '"'      ('\\' (([ \t]+ ('\r'? '\n')?)|.) | ~[\\\r\n"])*  '"'
           | '"""'    ('\\' .                          | ~'\\'     )*? '"""'
           | '\'\'\'' ('\\' .                          | ~'\\'     )*? '\'\'\''
           )
           ;
