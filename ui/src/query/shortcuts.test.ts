import { EditorState } from '@codemirror/state';
import type { Transaction } from '@codemirror/state';
import { runScopeHandlers } from '@codemirror/view';
import type { EditorView } from '@codemirror/view';
import { basicSetup } from 'codemirror';
import { describe, expect, test } from 'vitest';

import { queryRunKeymap } from './shortcuts';

describe('query editor shortcuts', () => {
  test.each([
    { shortcut: 'Command+Enter', ctrlKey: false, metaKey: true },
    { shortcut: 'Control+Enter', ctrlKey: true, metaKey: false },
  ])(
    'runs the query instead of an editor command on $shortcut',
    ({ ctrlKey, metaKey }) => {
      let runCount = 0;
      const editor = stateOnlyEditor(
        EditorState.create({
          doc: 'source demo_logs',
          extensions: [
            basicSetup,
            queryRunKeymap(() => {
              runCount += 1;
            }),
          ],
        }),
      );

      const handled = runScopeHandlers(
        editor.view,
        modifiedEnterEvent(ctrlKey, metaKey),
        'editor',
      );

      expect(handled).toBe(true);
      expect(runCount).toBe(1);
      expect(editor.document()).toBe('source demo_logs');
    },
  );
});

function stateOnlyEditor(initialState: EditorState): {
  readonly view: EditorView;
  readonly document: () => string;
} {
  let state = initialState;
  const view = {
    get state(): EditorState {
      return state;
    },
    dispatch(transaction: Transaction): void {
      state = transaction.state;
    },
  };
  return {
    // Keymap dispatch only reads state and dispatches a transaction. A DOM-backed
    // EditorView would make this unit test slower without changing the contract.
    view: view as unknown as EditorView,
    document: () => state.doc.toString(),
  };
}

function modifiedEnterEvent(ctrlKey: boolean, metaKey: boolean): KeyboardEvent {
  const event = {
    key: 'Enter',
    keyCode: 13,
    altKey: false,
    ctrlKey,
    metaKey,
    shiftKey: false,
    stopPropagation: () => undefined,
  };
  return event as unknown as KeyboardEvent;
}
