namespace Ameto.Tracing.TraceQL;

/// <summary>
/// Recursive-descent parser for TraceQL span-set filter expressions.
///
/// <para>Grammar:</para>
/// <code>
///   query      ::= '{' expr? '}'
///   expr       ::= or_expr
///   or_expr    ::= and_expr ( '||' and_expr )*
///   and_expr   ::= unary    ( '&&' unary    )*
///   unary      ::= '!' unary | primary
///   primary    ::= '(' expr ')' | attr_pred | intrinsic_pred
///   attr_pred  ::= ATTR op scalar        -- .key op value
///   intrinsic  ::= IDENT op scalar       -- duration/status/service/name/kind/http.status_code
///   op         ::= '=' | '!=' | '<' | '<=' | '>' | '>='
///   scalar     ::= STRING | NUMBER | DURATION | IDENT
/// </code>
/// </summary>
public static class TraceQLParser
{
    /// <exception cref="TraceQLException">On syntax error.</exception>
    public static SpanPredicate Parse(string query)
    {
        var tokens = TraceQLLexer.Tokenize(query.AsSpan());
        var p = new ParserState(tokens);
        p.Expect(TokenKind.LBrace);

        if (p.Peek().Kind == TokenKind.RBrace)
        {
            p.Consume();
            return TruePredicate.Instance;
        }

        var pred = p.ParseExpr();
        p.Expect(TokenKind.RBrace);
        return pred;
    }

    // ── Internal parser state ──────────────────────────────────────────────────

    private sealed class ParserState
    {
        private readonly List<Token> _tokens;
        private int _pos;

        public ParserState(List<Token> tokens) => _tokens = tokens;

        public Token Peek() => _pos < _tokens.Count ? _tokens[_pos] : new Token(TokenKind.Eof, "");

        public Token Consume()
        {
            var t = Peek();
            _pos++;
            return t;
        }

        public Token Expect(TokenKind kind)
        {
            var t = Peek();
            if (t.Kind != kind)
                throw new TraceQLException($"Expected {kind} but got {t.Kind}('{t.Text}') at position {_pos}");
            return Consume();
        }

        // expr = or_expr
        public SpanPredicate ParseExpr() => ParseOr();

        // or_expr = and_expr ( '||' and_expr )*
        private SpanPredicate ParseOr()
        {
            var left = ParseAnd();
            while (Peek().Kind == TokenKind.Or)
            {
                Consume();
                left = new OrPredicate(left, ParseAnd());
            }
            return left;
        }

        // and_expr = unary ( '&&' unary )*
        private SpanPredicate ParseAnd()
        {
            var left = ParseUnary();
            while (Peek().Kind == TokenKind.And)
            {
                Consume();
                left = new AndPredicate(left, ParseUnary());
            }
            return left;
        }

        // unary = '!' unary | primary
        private SpanPredicate ParseUnary()
        {
            if (Peek().Kind == TokenKind.Not)
            {
                Consume();
                return new NotPredicate(ParseUnary());
            }
            return ParsePrimary();
        }

        // primary = '(' expr ')' | attr_pred | intrinsic_pred
        private SpanPredicate ParsePrimary()
        {
            var t = Peek();

            if (t.Kind == TokenKind.LParen)
            {
                Consume();
                var inner = ParseExpr();
                Expect(TokenKind.RParen);
                return inner;
            }

            if (t.Kind == TokenKind.Attr)
            {
                Consume();
                string key = t.Text;
                var op     = ParseOp();
                var val    = ParseScalar();
                return BuildAttrPredicate(key, op, val);
            }

            if (t.Kind == TokenKind.Ident)
            {
                Consume();
                var op  = ParseOp();
                var val = ParseScalar();
                return BuildIntrinsicPredicate(t.Text, op, val);
            }

            throw new TraceQLException($"Unexpected token {t.Kind}('{t.Text}')");
        }

        private TraceQLOp ParseOp()
        {
            return Consume().Kind switch
            {
                TokenKind.Eq  => TraceQLOp.Eq,
                TokenKind.Neq => TraceQLOp.Neq,
                TokenKind.Lt  => TraceQLOp.Lt,
                TokenKind.Lte => TraceQLOp.Lte,
                TokenKind.Gt  => TraceQLOp.Gt,
                TokenKind.Gte => TraceQLOp.Gte,
                var k => throw new TraceQLException($"Expected comparison operator, got {k}"),
            };
        }

        private TraceQLValue ParseScalar()
        {
            var t = Consume();
            return t.Kind switch
            {
                TokenKind.String   => TraceQLValue.FromString(t.Text),
                TokenKind.Number   => TraceQLValue.FromNumber(t.Number),
                TokenKind.Duration => TraceQLValue.FromDuration((long)t.Number),
                TokenKind.Ident    => TraceQLValue.FromIdent(t.Text),
                _ => throw new TraceQLException($"Expected scalar value, got {t.Kind}('{t.Text}')"),
            };
        }

        // ── Predicate builders ──────────────────────────────────────────────────

        private static SpanPredicate BuildAttrPredicate(string key, TraceQLOp op, TraceQLValue val)
        {
            // Optimise: if key is a promoted field, use the fast predicate
            if (key is "http.status_code" or "http.response.status_code" && val.IsNumber)
                return new HttpStatusCodePredicate(op, (short)val.Number);

            return new AttributePredicate(key, op, val);
        }

        private static SpanPredicate BuildIntrinsicPredicate(string name, TraceQLOp op, TraceQLValue val)
        {
            switch (name.ToLowerInvariant())
            {
                case "duration":
                    if (!val.IsNumber)
                        throw new TraceQLException("duration requires a number or duration literal");
                    return new DurationPredicate(op, (long)val.Number);

                case "status":
                    var status = ParseStatus(val.StringVal ?? val.Number.ToString());
                    return new StatusPredicate(op, status);

                case "service" or "service.name":
                    return new ServicePredicate(op, val.StringVal ?? val.Number.ToString());

                case "name" or "span.name":
                    return new NamePredicate(op, val.StringVal ?? val.Number.ToString());

                case "kind" or "span.kind":
                    var kind = ParseKind(val.StringVal ?? val.Number.ToString());
                    return new KindPredicate(op, kind);

                default:
                    // Treat unknown intrinsic as attribute lookup
                    return new AttributePredicate(name, op, val);
            }
        }

        private static SpanStatusCode ParseStatus(string s) => s.ToLowerInvariant() switch
        {
            "error"       => SpanStatusCode.Error,
            "ok"          => SpanStatusCode.Ok,
            "unset"       => SpanStatusCode.Unset,
            "err"         => SpanStatusCode.Error,
            _             => SpanStatusCode.Unset,
        };

        private static SpanKind ParseKind(string s) => s.ToLowerInvariant() switch
        {
            "server"   => SpanKind.Server,
            "client"   => SpanKind.Client,
            "producer" => SpanKind.Producer,
            "consumer" => SpanKind.Consumer,
            "internal" => SpanKind.Internal,
            _          => SpanKind.Unspecified,
        };
    }
}

public sealed class TraceQLException(string message) : Exception(message);
