import { Prec } from '@codemirror/state';
import type { Extension } from '@codemirror/state';
import { keymap } from '@codemirror/view';

export function queryRunKeymap(onRun: () => void): Extension {
  const run = () => {
    onRun();
    return true;
  };
  return Prec.highest(
    keymap.of([
      { key: 'Meta-Enter', run },
      { key: 'Ctrl-Enter', run },
    ]),
  );
}
