#!/bin/bash

cd $(pwd)/parser
bison -t -d grammar.y && lex lexer.l
cd ..


