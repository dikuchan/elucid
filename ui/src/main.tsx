import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { MantineProvider } from '@mantine/core';
import '@mantine/core/styles.css';
import './index.css';

import { App } from './App';
import { theme } from './theme';

const root = document.querySelector('#root');

if (!(root instanceof HTMLElement)) {
  throw new Error('Elucid UI root element is missing');
}

createRoot(root).render(
  <StrictMode>
    <MantineProvider defaultColorScheme="dark" theme={theme}>
      <App />
    </MantineProvider>
  </StrictMode>,
);
