package cel

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser builds AST from tokens
type Parser struct {
	lexer *Lexer

	curToken  Token
	peekToken Token

	errors []string
}

func NewParser(input string) *Parser {
	p := &Parser{
		lexer:  NewLexer(input),
		errors: []string{},
	}

	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

func (p *Parser) Parse() (Expression, error) {
	expr := p.parseExpression(LOWEST)
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %v", p.errors)
	}
	return expr, nil
}

func (p *Parser) peekPrecedence() int {
	precedences := map[TokenType]int{
		QUESTION: TERNARY,
		OR:       OR_PRECEDENCE,
		AND:      AND_PRECEDENCE,
		EQ:       EQUALS,
		NE:       EQUALS,
		LT:       LESSGREATER,
		GT:       LESSGREATER,
		LE:       LESSGREATER,
		GE:       LESSGREATER,
		IN:       IN_PRECEDENCE,
		BETWEEN:  BETWEEN_PRECEDENCE,
		NOT:      IN_PRECEDENCE, // NOT IN and NOT BETWEEN need precedence for infix parsing
		LIKE:     LIKE_PRECEDENCE,
		PLUS:     SUM,
		MINUS:    SUM,
		MULTIPLY: PRODUCT,
		DIVIDE:   PRODUCT,
		MODULO:   PRODUCT,
		POWER:    POWER_PRECEDENCE,
		LPAREN:   CALL,
		LBRACKET: INDEX,
		DOT:      INDEX,
	}

	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) curPrecedence() int {
	precedences := map[TokenType]int{
		QUESTION: TERNARY,
		OR:       OR_PRECEDENCE,
		AND:      AND_PRECEDENCE,
		EQ:       EQUALS,
		NE:       EQUALS,
		LT:       LESSGREATER,
		GT:       LESSGREATER,
		LE:       LESSGREATER,
		GE:       LESSGREATER,
		IN:       IN_PRECEDENCE,
		BETWEEN:  BETWEEN_PRECEDENCE,
		NOT:      IN_PRECEDENCE, // NOT IN and NOT BETWEEN need precedence for infix parsing
		LIKE:     LIKE_PRECEDENCE,
		PLUS:     SUM,
		MINUS:    SUM,
		MULTIPLY: PRODUCT,
		DIVIDE:   PRODUCT,
		MODULO:   PRODUCT,
		POWER:    POWER_PRECEDENCE,
		LPAREN:   CALL,
		LBRACKET: INDEX,
		DOT:      INDEX,
	}

	if p, ok := precedences[p.curToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) parseExpression(precedence int) Expression {
	// Prefix parsing
	var leftExp Expression

	switch p.curToken.Type {
	case IDENTIFIER:
		leftExp = p.parseIdentifier()
	case NUMBER:
		leftExp = p.parseNumber()
	case STRING:
		leftExp = p.parseString()
	case TRUE, FALSE:
		leftExp = p.parseBoolean()
	case NULL:
		leftExp = p.parseNull()
	case NOT, MINUS:
		leftExp = p.parseUnaryExpression()
	case LPAREN:
		leftExp = p.parseGroupedExpression()
	case LBRACKET:
		leftExp = p.parseArrayLiteral()
	default:
		p.errors = append(p.errors, fmt.Sprintf("no prefix parse function for %v found", p.curToken.Type))
		return nil
	}

	// Infix parsing
	for p.peekToken.Type != EOF && precedence < p.peekPrecedence() {
		switch p.peekToken.Type {
		case QUESTION:
			p.nextToken()
			leftExp = p.parseTernaryExpression(leftExp)
		case PLUS, MINUS, MULTIPLY, DIVIDE, MODULO, POWER:
			p.nextToken()
			leftExp = p.parseBinaryExpression(leftExp)
		case EQ, NE, LT, GT, LE, GE:
			p.nextToken()
			leftExp = p.parseBinaryExpression(leftExp)
		case AND, OR:
			p.nextToken()
			leftExp = p.parseBinaryExpression(leftExp)
		case IN:
			p.nextToken()
			leftExp = p.parseBinaryExpression(leftExp)
		case BETWEEN:
			p.nextToken()
			leftExp = p.parseBetweenExpression(leftExp)
		case NOT:
			// Check what comes after NOT
			if p.peekToken.Type == IN {
				p.nextToken() // consume NOT
				p.nextToken() // consume IN
				leftExp = p.parseNotInExpression(leftExp)
			} else if p.peekToken.Type == BETWEEN {
				p.nextToken() // consume NOT
				p.nextToken() // consume BETWEEN
				leftExp = p.parseNotBetweenExpression(leftExp)
			} else if p.peekToken.Type == IDENTIFIER {
				// Check for "in" or "between" as identifiers
				if strings.ToLower(p.peekToken.Literal) == "in" {
					p.nextToken() // consume NOT
					p.nextToken() // consume "in"
					leftExp = p.parseNotInExpression(leftExp)
				} else if strings.ToLower(p.peekToken.Literal) == "between" {
					p.nextToken() // consume NOT
					p.nextToken() // consume "between"
					leftExp = p.parseNotBetweenExpression(leftExp)
				} else {
					return leftExp
				}
			} else {
				return leftExp
			}
		case LIKE:
			p.nextToken()
			leftExp = p.parseLikeExpression(leftExp)
		case LPAREN:
			p.nextToken()
			leftExp = p.parseCallExpression(leftExp)
		case LBRACKET:
			p.nextToken()
			leftExp = p.parseIndexExpression(leftExp)
		case DOT:
			p.nextToken()
			leftExp = p.parseFieldAccess(leftExp)
		default:
			return leftExp
		}
	}

	return leftExp
}

func (p *Parser) parseIdentifier() Expression {
	return &Variable{Name: p.curToken.Literal}
}

func (p *Parser) parseNumber() Expression {
	lit := &Literal{}

	if strings.Contains(p.curToken.Literal, ".") {
		value, err := strconv.ParseFloat(p.curToken.Literal, 64)
		if err != nil {
			msg := fmt.Sprintf("could not parse %q as float", p.curToken.Literal)
			p.errors = append(p.errors, msg)
			return nil
		}
		lit.Value = value
	} else {
		value, err := strconv.ParseInt(p.curToken.Literal, 0, 64)
		if err != nil {
			msg := fmt.Sprintf("could not parse %q as integer", p.curToken.Literal)
			p.errors = append(p.errors, msg)
			return nil
		}
		lit.Value = value
	}

	return lit
}

func (p *Parser) parseString() Expression {
	return &Literal{Value: p.curToken.Literal}
}

func (p *Parser) parseBoolean() Expression {
	return &Literal{Value: p.curToken.Type == TRUE}
}

func (p *Parser) parseNull() Expression {
	return &Literal{Value: nil}
}

func (p *Parser) parseUnaryExpression() Expression {
	expression := &UnaryOp{
		Op: p.curToken.Literal,
	}

	p.nextToken()

	expression.Expr = p.parseExpression(UNARY)

	return expression
}

func (p *Parser) parseGroupedExpression() Expression {
	p.nextToken()

	exp := p.parseExpression(LOWEST)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return exp
}

func (p *Parser) parseArrayLiteral() Expression {
	if p.peekToken.Type == RBRACKET {
		p.nextToken()
		return &ArrayLiteral{}
	}

	p.nextToken()

	// Check for list comprehension: [expr | var in collection, condition]
	firstExpr := p.parseExpression(LOWEST)
	if firstExpr == nil {
		return nil
	}

	if p.peekToken.Type == PIPE {
		// This is a list comprehension
		p.nextToken() // consume the '|'

		if !p.expectPeek(IDENTIFIER) {
			return nil
		}
		variable := p.curToken.Literal

		if !p.expectPeek(IN) {
			return nil
		}

		p.nextToken()
		collection := p.parseExpression(LOWEST)

		var condition Expression
		if p.peekToken.Type == COMMA {
			p.nextToken() // consume comma
			p.nextToken()
			condition = p.parseExpression(LOWEST)
		}

		if !p.expectPeek(RBRACKET) {
			return nil
		}

		return &Comprehension{
			Expression: firstExpr,
			Variable:   variable,
			Collection: collection,
			Condition:  condition,
		}
	}

	// Regular array literal
	array := &ArrayLiteral{}
	array.Elements = append(array.Elements, firstExpr)

	for p.peekToken.Type == COMMA {
		p.nextToken() // consume comma
		p.nextToken() // move to next element
		nextExpr := p.parseExpression(LOWEST)
		if nextExpr == nil {
			return nil
		}
		array.Elements = append(array.Elements, nextExpr)
	}

	if !p.expectPeek(RBRACKET) {
		return nil
	}

	return array
}

func (p *Parser) parseBinaryExpression(left Expression) Expression {
	expression := &BinaryOp{
		Left: left,
		Op:   p.curToken.Literal,
	}

	precedence := p.curPrecedence()
	p.nextToken()
	expression.Right = p.parseExpression(precedence)

	return expression
}

func (p *Parser) parseBetweenExpression(left Expression) Expression {
	expression := &BetweenOp{
		Value: left,
	}

	// Expect the first operand after BETWEEN
	p.nextToken()
	expression.Low = p.parseExpression(BETWEEN_PRECEDENCE)

	// Expect AND keyword (could be identifier "and" or logical AND "&&")
	if p.peekToken.Type == IDENTIFIER && strings.ToLower(p.peekToken.Literal) == "and" {
		p.nextToken() // consume "and"
	} else if !p.expectPeek(AND) {
		return nil
	}

	// Expect the second operand after AND
	p.nextToken()
	expression.High = p.parseExpression(BETWEEN_PRECEDENCE)

	return expression
}

func (p *Parser) parseNotInExpression(left Expression) Expression {
	expression := &NotInOp{
		Left: left,
	}

	// Expect the right operand after NOT IN
	p.nextToken()
	expression.Right = p.parseExpression(IN_PRECEDENCE)

	return expression
}

func (p *Parser) parseNotBetweenExpression(left Expression) Expression {
	expression := &NotBetweenOp{
		Value: left,
	}

	// Expect the first operand after NOT BETWEEN
	p.nextToken()
	expression.Low = p.parseExpression(BETWEEN_PRECEDENCE)

	// Expect AND keyword (could be identifier "and" or logical AND "&&")
	if p.peekToken.Type == IDENTIFIER && strings.ToLower(p.peekToken.Literal) == "and" {
		p.nextToken() // consume "and"
	} else if !p.expectPeek(AND) {
		return nil
	}

	// Expect the second operand after AND
	p.nextToken()
	expression.High = p.parseExpression(BETWEEN_PRECEDENCE)

	return expression
}

func (p *Parser) parseLikeExpression(left Expression) Expression {
	expression := &LikeOp{
		Value: left,
	}

	// Expect the pattern after LIKE
	p.nextToken()
	expression.Pattern = p.parseExpression(LIKE_PRECEDENCE)

	return expression
}

func (p *Parser) parseTernaryExpression(condition Expression) Expression {
	p.nextToken() // consume the '?'

	trueExpr := p.parseExpression(LOWEST)

	if !p.expectPeek(COLON) {
		return nil
	}

	p.nextToken() // consume the ':'
	falseExpr := p.parseExpression(TERNARY)

	return &TernaryOp{
		Condition: condition,
		TrueExpr:  trueExpr,
		FalseExpr: falseExpr,
	}
}

func (p *Parser) parseCallExpression(fn Expression) Expression {
	exp := &FunctionCall{}

	// Check if this is a method call on an object
	if variable, ok := fn.(*Variable); ok {
		exp.Name = variable.Name
	} else {
		p.errors = append(p.errors, "invalid function call")
		return nil
	}

	exp.Args = p.parseExpressionList(RPAREN)
	return exp
}

func (p *Parser) parseIndexExpression(left Expression) Expression {
	exp := &IndexAccess{Object: left}

	p.nextToken()
	exp.Index = p.parseExpression(LOWEST)

	if !p.expectPeek(RBRACKET) {
		return nil
	}

	return exp
}

func (p *Parser) parseFieldAccess(left Expression) Expression {
	if p.peekToken.Type != IDENTIFIER {
		p.errors = append(p.errors, fmt.Sprintf("expected identifier after dot, got %v", p.peekToken.Type))
		return nil
	}

	p.nextToken()
	fieldName := p.curToken.Literal

	// Check for macro operations FIRST (collection.filter, .map, etc.)
	if fieldName == "filter" || fieldName == "map" || fieldName == "all" || fieldName == "exists" || fieldName == "find" ||
		fieldName == "size" || fieldName == "reverse" || fieldName == "sort" || fieldName == "flatMap" || fieldName == "groupBy" {
		return p.parseMacroExpression(left, fieldName)
	}

	// Check for method calls (field followed by parentheses)
	if p.peekToken.Type == LPAREN {
		p.nextToken() // consume the '('

		method := &MethodCall{
			Object: left,
			Method: fieldName,
		}

		method.Args = p.parseExpressionList(RPAREN)
		return method
	}

	// Regular field access
	return &FieldAccess{
		Object: left,
		Field:  fieldName,
	}
}

func (p *Parser) parseMacroExpression(collection Expression, macroType string) Expression {
	if !p.expectPeek(LPAREN) {
		return nil
	}

	// Some operations don't need variables (size, reverse)
	if macroType == "size" || macroType == "reverse" {
		if !p.expectPeek(RPAREN) {
			return nil
		}
		return &Macro{
			Collection: collection,
			Variable:   "",
			Body:       nil,
			Type:       macroType,
		}
	}

	if !p.expectPeek(IDENTIFIER) {
		return nil
	}

	variable := p.curToken.Literal

	if !p.expectPeek(COMMA) {
		return nil
	}

	p.nextToken()
	body := p.parseExpression(LOWEST)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return &Macro{
		Collection: collection,
		Variable:   variable,
		Body:       body,
		Type:       macroType,
	}
}

func (p *Parser) parseExpressionList(end TokenType) []Expression {
	var args []Expression

	if p.peekToken.Type == end {
		p.nextToken()
		return args
	}

	p.nextToken()
	args = append(args, p.parseExpression(LOWEST))

	for p.peekToken.Type == COMMA {
		p.nextToken()
		p.nextToken()
		args = append(args, p.parseExpression(LOWEST))
	}

	if !p.expectPeek(end) {
		return nil
	}

	return args
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekToken.Type == t {
		p.nextToken()
		return true
	} else {
		p.peekError(t)
		return false
	}
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %v, got %v instead", t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

// BetweenOp represents a BETWEEN expression (value BETWEEN low AND high)
type BetweenOp struct {
	Value Expression
	Low   Expression
	High  Expression
}

func (b *BetweenOp) Evaluate(ctx *Context) (Value, error) {
	value, err := b.Value.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	low, err := b.Low.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	high, err := b.High.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	// Check if value is between low and high using proper type-aware comparison
	return compare(value, low) >= 0 && compare(value, high) <= 0, nil
}

// LikeOp represents a LIKE expression (value LIKE pattern)
type LikeOp struct {
	Value   Expression
	Pattern Expression
}

func (l *LikeOp) Evaluate(ctx *Context) (Value, error) {
	value, err := l.Value.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	pattern, err := l.Pattern.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	// Convert to strings
	str := toString(value)
	pat := toString(pattern)

	// Use the existing matchPattern function from utils.go
	return matchPattern(str, pat), nil
}
