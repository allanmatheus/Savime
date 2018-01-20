/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_YY_BISON_H_INCLUDED
# define YY_YY_BISON_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 1
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    ASTERISK = 258,
    COMMA = 259,
    PERIOD = 260,
    COLON = 261,
    SEMICOLON = 262,
    PLUS_SIGN = 263,
    MINUS_SIGN = 264,
    SOLIDUS = 265,
    MODULUS = 266,
    POWER = 267,
    OR = 268,
    AND = 269,
    LEFT_PAREN = 270,
    RIGHT_PAREN = 271,
    LEFT_ANGLE_BRACKETS = 272,
    RIGHT_ANGLE_BRACKETS = 273,
    DOUBLE_QUOTE = 274,
    IS = 275,
    DOUBLE_PIPE = 276,
    RIGHT_ARROW = 277,
    IDENTIFIER_BODY = 278,
    TRUE = 279,
    FALSE = 280,
    UNKNOWN = 281,
    B = 282,
    BITS = 283,
    X = 284,
    HEX = 285,
    BETWEEN = 286,
    IN = 287,
    EXACT_NUMBER = 288,
    SIGNED_EXACT_NUMBER = 289,
    APROX_NUMBER = 290,
    SIGNED_APROX_NUMBER = 291,
    STRING = 292,
    QUOTE = 293,
    EQ = 294,
    NEQ = 295,
    LE = 296,
    GE = 297,
    LEQ = 298,
    GEQ = 299,
    NEWLINE = 300,
    NOT = 301
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 85 "grammar.y" /* yacc.c:1909  */

	float num;
	char * value;
	ParseTreeNodePtr  * node;
	QueryExpressionPtr   * queryExpression;
	ValueExpressionPtr* valueExpression;
	IdentifierPtr  * identifier;
	UnsignedLiteralPtr  * unsignedLiteral;
	std::shared_ptr<ValueExpressionList> * valueExpressionList;
	std::shared_ptr<GeneralLiteral> * generalLiteral;
	IdentifierChainPtr  * identifierChain;
	UnsignedNumericLiteralPtr  * unsignedNumericLiteral;
        SignedNumericLiteralPtr  * signedNumericLiteral;
	CharacterStringLiteralPtr  * characterStringLiteral;
	std::shared_ptr<BitStringLiteral> * bitStringLiteral;
	std::shared_ptr<HexStringLiteral> * hexStringLiteral;
	std::shared_ptr<CompOp> * compOp;

#line 120 "bison.h" /* yacc.c:1909  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (void * scanner, void ** ret);

#endif /* !YY_YY_BISON_H_INCLUDED  */
