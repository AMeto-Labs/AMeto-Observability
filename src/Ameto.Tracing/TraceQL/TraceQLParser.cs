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
    /// <summary>
    /// The longest query text this parser will look at.
    ///
    /// <para>A ceiling on the LEXER, which the depth counter below cannot bound: tokenising is
    /// iterative and safe at any length, but it materialises one <c>Token</c> — a kind, a string
    /// and a double — per symbol, so a 30 MB <c>POST /api/traces/query</c> body is hundreds of
    /// megabytes of token list before the parser is handed anything to refuse. On the 512 MB box
    /// this branch exists to keep alive that is the whole machine.</para>
    ///
    /// <para>8 KB is far past any real filter — the longest one in this repo's tests is under
    /// 200 characters — and it is roughly what Kestrel's default 8192-byte request line already
    /// imposes on the <c>?ql=</c> form. This makes the POST form obey the same bound rather than
    /// inheriting a 30 MB one.</para>
    /// </summary>
    public const int MaxQueryChars = 8 * 1024;

    /// <exception cref="TraceQLException">On syntax error.</exception>
    public static SpanPredicate Parse(string query)
    {
        if (query.Length > MaxQueryChars)
            throw new TraceQLException(
                $"Query is {query.Length} characters; the limit is {MaxQueryChars}");

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
        /// <summary>
        /// How deeply the grammar may nest before the query is refused.
        ///
        /// <para>THIS IS NOT A TASTE LIMIT, IT IS THE ONLY THING BETWEEN A QUERY STRING AND THE
        /// PROCESS. <c>StackOverflowException</c> cannot be caught in .NET: the runtime kills the
        /// whole process, taking every other in-flight request, the ingest pipeline and every
        /// unwritten WAL buffer with it. Measured against the parent commit: <c>{((((…))))}</c>
        /// survives 1500 levels and dies at 1700 — <b>a 3406-character query</b>, which fits
        /// comfortably inside Kestrel's default 8192-byte request line, so one
        /// <c>GET /api/traces/query/stream?ql=</c> from a browser address bar was enough. The
        /// <c>!</c> form needs ~20 000 characters and so is only reachable through the POST body,
        /// which this bounds too.</para>
        ///
        /// <para>64 against a real query's 2 or 3. Anything deeper is not a filter somebody wrote;
        /// and the refusal is a <see cref="TraceQLException"/>, which both entry points already
        /// turn into a 400 (or a <c>query-error</c> frame on the stream), so a mistake here costs a
        /// message and not a connection.</para>
        /// </summary>
        private const int MaxDepth = 64;

        private readonly List<Token> _tokens;
        private int _pos;
        private int _depth;

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
        //
        // THE ONE CHOKE POINT, which is why the counter lives here and nowhere else. Every
        // recursive edge in this grammar passes through this method exactly once per level of
        // nesting: '!' recurses straight back into it, and '(' goes ParsePrimary → ParseExpr →
        // ParseOr → ParseAnd → here. A counter in each recursive method would be three places to
        // keep in step; this is one, and a rule added later inherits the bound as long as it
        // reaches its operand through unary — which every operand in this grammar does.
        //
        // The decrement is in a `finally` because SIBLINGS ARE NOT NESTING. `{(a=1) && (b=2) &&
        // …}` opens and closes one level at a time; without the restore a flat query of 65 terms
        // would be refused as if it were 65 deep, which is a working query rejected in the name of
        // a crash it cannot cause.
        private SpanPredicate ParseUnary()
        {
            if (++_depth > MaxDepth)
                throw new TraceQLException(
                    $"Query nests more than {MaxDepth} levels deep at position {_pos}");
            try
            {
                if (Peek().Kind == TokenKind.Not)
                {
                    Consume();
                    return new NotPredicate(ParseUnary());
                }
                return ParsePrimary();
            }
            finally { _depth--; }
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
