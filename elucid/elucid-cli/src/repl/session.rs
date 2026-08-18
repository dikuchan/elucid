use elucid_engine::Context;
use reedline::{DefaultPrompt, DefaultPromptSegment, Emacs, Reedline, Signal};

use super::highlighter::QueryHighlighter;
use super::validator::QueryValidator;

pub async fn start(context: &Context) -> anyhow::Result<()> {
    let mut line_editor = Reedline::create()
        .with_highlighter(Box::new(QueryHighlighter))
        .with_validator(Box::new(QueryValidator))
        .with_edit_mode(Box::new(Emacs::default()));

    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic(">>>".to_owned()),
        DefaultPromptSegment::Empty,
    );

    println!("Elucid REPL v0.1.0");
    println!("Type 'exit' or Ctrl-C to quit.");

    loop {
        let signal = line_editor.read_line(&prompt);
        match signal {
            Ok(Signal::Success(buffer)) => {
                let input = buffer.trim();
                if input.is_empty() {
                    continue;
                }
                if input == "exit" {
                    break;
                }
                if input == "clear" {
                    line_editor.clear_scrollback()?;
                    continue;
                }

                match context.execute(input).await {
                    Ok(data) => {
                        data.show().await?;
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                    }
                }
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => {
                println!("Bye!");
                break;
            }
            Err(error) => {
                eprintln!("REPL Error: {:?}", error);
                break;
            }
        }
    }

    Ok(())
}
