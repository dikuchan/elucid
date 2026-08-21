import { useEffect, useMemo, useRef } from 'react';
import { lintGutter, setDiagnostics } from '@codemirror/lint';
import { Compartment, EditorState } from '@codemirror/state';
import { EditorView, keymap } from '@codemirror/view';
import { basicSetup } from 'codemirror';

import type { QueryDiagnostic } from '../api/contracts';
import classes from '../App.module.css';
import { diagnosticsForEditor } from '../query/diagnostics';

interface QueryEditorProps {
  readonly value: string;
  readonly diagnostics: readonly QueryDiagnostic[];
  readonly disabled: boolean;
  readonly onChange: (value: string) => void;
  readonly onRun: () => void;
}

export function QueryEditor({
  value,
  diagnostics,
  disabled,
  onChange,
  onRun,
}: QueryEditorProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  const onRunRef = useRef(onRun);
  const initialValueRef = useRef(value);
  const editableCompartment = useMemo(() => new Compartment(), []);

  useEffect(() => {
    onChangeRef.current = onChange;
    onRunRef.current = onRun;
  }, [onChange, onRun]);

  useEffect(() => {
    const parent = containerRef.current;
    if (parent === null) {
      return undefined;
    }
    const view = new EditorView({
      parent,
      state: EditorState.create({
        doc: initialValueRef.current,
        extensions: [
          basicSetup,
          lintGutter(),
          EditorView.lineWrapping,
          editableCompartment.of(EditorView.editable.of(true)),
          EditorView.contentAttributes.of({
            'aria-label': 'Query editor',
            'aria-keyshortcuts': 'Control+Enter Meta+Enter',
          }),
          keymap.of([
            {
              key: 'Mod-Enter',
              run: () => {
                onRunRef.current();
                return true;
              },
            },
          ]),
          EditorView.updateListener.of((update) => {
            if (update.docChanged) {
              onChangeRef.current(update.state.doc.toString());
            }
          }),
        ],
      }),
    });
    viewRef.current = view;
    return () => {
      viewRef.current = null;
      view.destroy();
    };
  }, [editableCompartment]);

  useEffect(() => {
    const view = viewRef.current;
    if (view === null) {
      return;
    }
    const currentValue = view.state.doc.toString();
    if (currentValue !== value) {
      view.dispatch({
        changes: { from: 0, to: currentValue.length, insert: value },
      });
    }
  }, [value]);

  useEffect(() => {
    const view = viewRef.current;
    if (view !== null) {
      view.dispatch({
        effects: editableCompartment.reconfigure(
          EditorView.editable.of(!disabled),
        ),
      });
    }
  }, [disabled, editableCompartment]);

  useEffect(() => {
    const view = viewRef.current;
    if (view !== null) {
      view.dispatch(
        setDiagnostics(view.state, [
          ...diagnosticsForEditor(value, diagnostics),
        ]),
      );
    }
  }, [diagnostics, value]);

  return <div ref={containerRef} className={classes.queryEditor} />;
}
