 {
        open Parser        (* The type token is defined in parser.mli *)
(*		exception Eof
*)
 }

  rule token = parse
      [' ' '\t']     				{ token lexbuf }    (* skip blanks *)
    | ['\n' ]        				{ token lexbuf }    (* skip newline *)
    | '%'[^'\n''v''s'][^'\n']*'\n'        			{ token lexbuf }    (* skip comments *)
    | '%'['v''s'][^':'][^'\n']*'\n'        			{ token lexbuf }    (* skip comments *)
    | ['0'-'9']+ as lxm 			{ VAL(int_of_string lxm) }
    | '\''(('\'''\'')|[^'\n''\''])*'\'' as lxm  { STRING(lxm) }
    | ['a'-'z']['a'-'z''0'-'9''_']* as lxm 	{ match lxm with 
                            | "and" -> AND 
                            | "not" -> NOT 
                            | _ -> RELNAME(lxm) }
    | ['A'-'Z']['A'-'Z''0'-'9''_']* as lxm 	{ match lxm with
						    | "AND" -> AND
						    | "NOT" -> NOT
						    | _     -> VARNAME(lxm)
						}
    | ":-"          				{ IMPLIEDBY }
    | "?-"            				{ QMARK }  (* query mark *)
    | "%v:"            				{ QMARK }  (* query mark *)
    | "%s:"                         {UMARK} (* updated base relation mark *)
    | '.'            				{ DOT }    (* end of rule or query *)
    | ','            				{ SEP }
    | '('            				{ LPAREN }
    | ')'            				{ RPAREN }
    | '='            				{ EQ }
    | "<>"            				{ NE }
    | '/'            				{ EOP }
    | '_'                                       { ANONVAR }
    | "<="                                      { LE }
    | ">="                                      { GE }
    | '<'                                       { LT }
    | '>'                                       { GT }
    | '+'                                       { PLUS }
    | '-'                                       { MINUS }
	| eof            { EOF }
	