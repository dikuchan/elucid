use elucid_language::lexer::{Token, tokenizer};
use nu_ansi_term::{Color, Style};
use reedline::{Highlighter, StyledText};

pub struct QueryHighlighter;

impl Highlighter for QueryHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled_text = StyledText::new();
        let mut last_end = 0;

        for (token, span) in tokenizer(line) {
            if span.start() > last_end {
                styled_text.push((Style::new(), line[last_end..span.start()].to_owned()));
            }

            let style = match token {
                Token::KeywordDataset
                | Token::KeywordFields
                | Token::KeywordWhere
                | Token::KeywordSort
                | Token::KeywordHead
                | Token::KeywordStats
                | Token::KeywordBy
                | Token::KeywordNot
                | Token::KeywordNull
                | Token::KeywordTrue
                | Token::KeywordFalse => Style::new().fg(Color::LightBlue).bold(),

                Token::OperatorAdd
                | Token::OperatorSubtract
                | Token::OperatorMultiply
                | Token::OperatorDivide
                | Token::OperatorEqual
                | Token::OperatorNotEqual
                | Token::OperatorGreaterThan
                | Token::OperatorGreaterThanOrEqual
                | Token::OperatorLessThan
                | Token::OperatorLessThanOrEqual
                | Token::OperatorAnd
                | Token::OperatorOr
                | Token::OperatorAssign
                | Token::Pipe => Style::new().fg(Color::Yellow),

                Token::StringLiteral(_) => Style::new().fg(Color::Green),
                Token::Integer(_) => Style::new().fg(Color::LightCyan),

                _ => Style::new().fg(Color::White),
            };

            styled_text.push((style, line[span.start()..span.end()].to_owned()));
            last_end = span.end();
        }

        if last_end < line.len() {
            styled_text.push((Style::new(), line[last_end..].to_owned()));
        }

        styled_text
    }
}
