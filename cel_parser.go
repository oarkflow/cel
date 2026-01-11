package cel

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Tokenize the expression into tokens
func (p *Parser) tokenize() ([]Token, error) {
	var tokens []Token
	i := 0

	for i < len(p.expr) {
		char := p.expr[i]

		// Skip whitespace
		if unicode.IsSpace(rune(char)) {
			i++
			continue
		}

		// String literals
		if char == '"' || char == '\'' {
			token, err := p.parseStringLiteral(i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token)
			i = token.Pos + len(token.Value) + 2 // Account for opening and closing quotes
			continue
		}

		// Numbers
		if isDigit(char) || (char == '.' && i+1 < len(p.expr) && isDigit(p.expr[i+1])) {
			token, err := p.parseNumberLiteral(i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token)
			i = token.Pos + len(token.Value)
			continue
		}

		// Identifiers and keywords
		if isLetter(char) || char == '_' {
			token, err := p.parseIdentifier(i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token)
			i = token.Pos + len(token.Value)
			continue
		}

		// Operators and punctuation
		token, advance := p.parseOperatorOrPunctuation(i)
		if token.Type != TokenEOF {
			tokens = append(tokens, token)
			i += advance
			continue
		}

		return nil, fmt.Errorf("unexpected character: %c", char)
	}

	tokens = append(tokens, Token{Type: TokenEOF, Value: "", Pos: len(p.expr)})
	return tokens, nil
}

func (p *Parser) parseStringLiteral(pos int) (Token, error) {
	quote := p.expr[pos]
	start := pos + 1
	i := start

	for i < len(p.expr) && p.expr[i] != quote {
		if p.expr[i] == '\\' && i+1 < len(p.expr) {
			i += 2 // Skip escaped character
			continue
		}
		i++
	}

	if i >= len(p.expr) {
		return Token{}, fmt.Errorf("unterminated string literal")
	}

	value := p.expr[start:i]
	i++ // Skip closing quote

	// Process escape sequences
	value = strings.ReplaceAll(value, "\\n", "\n")
	value = strings.ReplaceAll(value, "\\t", "\t")
	value = strings.ReplaceAll(value, "\\\"", "\"")
	value = strings.ReplaceAll(value, "\\'", "'")
	value = strings.ReplaceAll(value, "\\\\", "\\")

	return Token{Type: TokenString, Value: value, Pos: pos}, nil
}

func (p *Parser) parseNumberLiteral(pos int) (Token, error) {
	start := pos
	i := pos

	// Parse integer part
	for i < len(p.expr) && (isDigit(p.expr[i]) || p.expr[i] == '_') {
		i++
	}

	// Parse decimal part
	if i < len(p.expr) && p.expr[i] == '.' {
		i++
		for i < len(p.expr) && (isDigit(p.expr[i]) || p.expr[i] == '_') {
			i++
		}
	}

	// Parse exponent
	if i < len(p.expr) && (p.expr[i] == 'e' || p.expr[i] == 'E') {
		i++
		if i < len(p.expr) && (p.expr[i] == '+' || p.expr[i] == '-') {
			i++
		}
		for i < len(p.expr) && isDigit(p.expr[i]) {
			i++
		}
	}

	value := p.expr[start:i]
	return Token{Type: TokenNumber, Value: value, Pos: pos}, nil
}

func (p *Parser) parseIdentifier(pos int) (Token, error) {
	start := pos
	i := pos

	for i < len(p.expr) && (isLetter(p.expr[i]) || isDigit(p.expr[i]) || p.expr[i] == '_') {
		i++
	}

	value := p.expr[start:i]

	// Check if it's a keyword
	keywords := map[string]bool{
		"true":    true,
		"false":   true,
		"null":    true,
		"in":      true,
		"between": true,
		"filter":  true,
		"map":     true,
		"all":     true,
		"exists":  true,
		"find":    true,
		"size":    true,
		"length":  true,
		"first":   true,
		"last":    true,
	}

	tokenType := TokenIdentifier
	if keywords[value] {
		tokenType = TokenKeyword
	}

	return Token{Type: tokenType, Value: value, Pos: pos}, nil
}

func (p *Parser) parseOperatorOrPunctuation(pos int) (Token, int) {
	char := p.expr[pos]

	// Multi-character operators
	if pos+1 < len(p.expr) {
		twoChar := p.expr[pos : pos+2]
		switch twoChar {
		case "==", "!=", "<=", ">=", "&&", "||":
			return Token{Type: TokenOperator, Value: twoChar, Pos: pos}, 2
		}
	}

	// Single character operators and punctuation
	switch char {
	case '+', '-', '*', '/', '%', '^', '<', '>', '!':
		return Token{Type: TokenOperator, Value: string(char), Pos: pos}, 1
	case '(', ')', '[', ']', '{', '}', ',', ':', '?', ';':
		return Token{Type: TokenPunctuation, Value: string(char), Pos: pos}, 1
	}

	return Token{Type: TokenEOF, Value: "", Pos: pos}, 1
}

// Parse expression with operator precedence
func (p *Parser) parseExpression(precedence int) (ASTNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		op, ok := p.peekOperator()
		if !ok {
			break
		}

		opPrec := getOperatorPrecedence(op)
		if opPrec < precedence {
			break
		}

		p.nextToken() // consume operator

		right, err := p.parseExpression(opPrec + 1)
		if err != nil {
			return nil, err
		}

		left = &BinaryOp{Op: op, Left: left, Right: right}
	}

	return left, nil
}

func (p *Parser) parseUnary() (ASTNode, error) {
	// Handle unary operators
	if op, ok := p.peekOperator(); ok && (op == "-" || op == "!") {
		p.nextToken() // consume operator
		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryOp{Op: op, Expr: operand}, nil
	}

	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (ASTNode, error) {
	token := p.nextToken()

	switch token.Type {
	case TokenNumber:
		value, err := strconv.ParseFloat(token.Value, 64)
		if err != nil {
			return nil, err
		}
		return &NumberLiteral{Value: value, raw: token.Value}, nil

	case TokenString:
		return &StringLiteral{Value: token.Value, raw: token.Value}, nil

	case TokenKeyword:
		switch token.Value {
		case "true":
			return &BooleanLiteral{Value: true, raw: token.Value}, nil
		case "false":
			return &BooleanLiteral{Value: false, raw: token.Value}, nil
		case "null":
			return &NullLiteral{Value: nil}, nil
		default:
			// For collection operations and other keywords, treat as identifier
			return p.parseIdentifierOrFunctionCall(token)
		}

	case TokenIdentifier:
		return p.parseIdentifierOrFunctionCall(token)

	case TokenPunctuation:
		if token.Value == "(" {
			expr, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}

			if p.peekToken().Type != TokenPunctuation || p.peekToken().Value != ")" {
				return nil, fmt.Errorf("expected ')'")
			}
			p.nextToken() // consume ')'
			return expr, nil
		}

	}

	return nil, fmt.Errorf("unexpected token: %v", token)
}

func (p *Parser) parseIdentifierOrFunctionCall(ident Token) (ASTNode, error) {
	// Check if it's a function call
	if p.peekToken().Type == TokenPunctuation && p.peekToken().Value == "(" {
		p.nextToken() // consume '('

		args, err := p.parseArgumentList()
		if err != nil {
			return nil, err
		}

		return &FunctionCall{Name: ident.Value, Arguments: args}, nil
	}

	// Check if it's a method call
	if p.peekToken().Type == TokenIdentifier && p.peekToken().Value == "." {
		p.nextToken() // consume identifier
		nextIdent := p.nextToken()

		if nextIdent.Type != TokenIdentifier {
			return nil, fmt.Errorf("expected method name")
		}

		var methodArgs []ASTNode
		if p.peekToken().Type == TokenPunctuation && p.peekToken().Value == "(" {
			p.nextToken() // consume '('
			args, err := p.parseArgumentList()
			if err != nil {
				return nil, err
			}
			methodArgs = args

			if p.peekToken().Type != TokenPunctuation || p.peekToken().Value != ")" {
				return nil, fmt.Errorf("expected ')'")
			}
			p.nextToken() // consume ')'
		}

		return &MethodCall{
			Object:    &Identifier{Name: ident.Value},
			Method:    nextIdent.Value,
			Arguments: methodArgs,
		}, nil
	}

	// Check for collection operations (filter, map, all, exists, find, size, first, last)
	collectionOps := map[string]bool{
		"filter": true, "map": true, "all": true, "exists": true, "find": true,
		"size": true, "length": true, "first": true, "last": true,
	}
	if collectionOps[ident.Value] && p.peekToken().Type == TokenPunctuation && p.peekToken().Value == "(" {
		return p.parseCollectionOperation(ident.Value)
	}

	return &Identifier{Name: ident.Value}, nil
}

func (p *Parser) parseArgumentList() ([]ASTNode, error) {
	var args []ASTNode

	// Check if we have a closing parenthesis immediately

	if p.peekToken().Type == TokenPunctuation && p.peekToken().Value == ")" {
		p.nextToken() // consume ')'
		return args, nil
	}

	for {
		arg, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		if p.peekToken().Type == TokenPunctuation && p.peekToken().Value == "," {
			p.nextToken() // consume ','
			continue
		}

		if p.peekToken().Type == TokenPunctuation && p.peekToken().Value == ")" {
			p.nextToken() // consume ')'
			break
		}

		return nil, fmt.Errorf("expected ',' or ')'")
	}

	return args, nil
}

func (p *Parser) parseCollectionOperation(operation string) (ASTNode, error) {
	// Parse opening parenthesis
	if p.peekToken().Type != TokenPunctuation || p.peekToken().Value != "(" {
		return nil, fmt.Errorf("expected '('")
	}
	p.nextToken() // consume '('

	// Check for simple operations that take single argument
	if operation == "size" || operation == "first" || operation == "last" {
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}

		if p.peekToken().Type != TokenPunctuation || p.peekToken().Value != ")" {
			return nil, fmt.Errorf("expected ')'")
		}
		p.nextToken() // consume ')'

		switch operation {
		case "size":
			return &Size{Expr: expr}, nil
		case "first":
			return &First{Expr: expr}, nil
		case "last":
			return &Last{Expr: expr}, nil
		}
	}

	// Parse variable name for complex operations
	if p.peekToken().Type != TokenIdentifier {
		return nil, fmt.Errorf("expected variable name")
	}
	variable := p.nextToken()

	// Parse comma
	if p.peekToken().Type != TokenPunctuation || p.peekToken().Value != "," {
		return nil, fmt.Errorf("expected ','")
	}
	p.nextToken() // consume ','

	// Parse source expression
	source, err := p.parseExpression(0)
	if err != nil {
		return nil, err
	}

	// Check for predicate (some operations need it)
	var predicate ASTNode
	if p.peekToken().Type == TokenPunctuation && p.peekToken().Value == "," {
		p.nextToken() // consume ','
		predicate, err = p.parseExpression(0)
		if err != nil {
			return nil, err
		}
	}

	// Parse closing parenthesis
	if p.peekToken().Type != TokenPunctuation || p.peekToken().Value != ")" {
		return nil, fmt.Errorf("expected ')'")
	}
	p.nextToken() // consume ')'

	switch operation {
	case "filter":
		if predicate == nil {
			return nil, fmt.Errorf("filter requires predicate")
		}
		return &Filter{
			Variable:  variable.Value,
			Source:    source,
			Predicate: predicate,
		}, nil
	case "map":
		if predicate == nil {
			return nil, fmt.Errorf("map requires transform function")
		}
		return &Map{
			Variable:  variable.Value,
			Source:    source,
			Transform: predicate,
		}, nil
	case "all":
		if predicate == nil {
			return nil, fmt.Errorf("all requires predicate")
		}
		return &All{
			Variable:  variable.Value,
			Source:    source,
			Predicate: predicate,
		}, nil
	case "exists":
		if predicate == nil {
			return nil, fmt.Errorf("exists requires predicate")
		}
		return &Exists{
			Variable:  variable.Value,
			Source:    source,
			Predicate: predicate,
		}, nil
	case "find":
		if predicate == nil {
			return nil, fmt.Errorf("find requires predicate")
		}
		return &Find{
			Variable:  variable.Value,
			Source:    source,
			Predicate: predicate,
		}, nil
	default:
		return nil, fmt.Errorf("unknown collection operation: %s", operation)
	}
}

// Token parsing helpers
func (p *Parser) peekToken() Token {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return Token{Type: TokenEOF, Value: "", Pos: len(p.expr)}
}

func (p *Parser) nextToken() Token {
	if p.pos < len(p.tokens) {
		token := p.tokens[p.pos]
		p.pos++
		return token
	}
	return Token{Type: TokenEOF, Value: "", Pos: len(p.expr)}
}

func (p *Parser) peekOperator() (string, bool) {
	token := p.peekToken()
	if token.Type == TokenOperator {
		return token.Value, true
	}
	return "", false
}

// Helper functions
func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func getOperatorPrecedence(op string) int {
	switch op {
	case "||":
		return 1
	case "&&":
		return 2
	case "==", "!=":
		return 3
	case "<", ">", "<=", ">=":
		return 4
	case "+", "-":
		return 5
	case "*", "/", "%":
		return 6
	case "^":
		return 7
	default:
		return 0
	}
}
