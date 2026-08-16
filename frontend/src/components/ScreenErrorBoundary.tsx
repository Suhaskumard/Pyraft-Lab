/**
 * Contains a render failure to the screen that caused it.
 *
 * Without this, one screen throwing takes the whole application with it: React unmounts
 * the tree, so the nav, the header and the live socket all disappear and the page goes
 * blank with nothing on it explaining why. That is a bad trade for a bug in one panel —
 * the other seventeen screens were fine, and the cluster behind them was still up.
 *
 * A class component because that is still the only way to catch a render error; there
 * is no hook equivalent. `resetKey` changes when the user navigates, which clears the
 * error so a screen is retried on its next visit rather than staying broken until a
 * reload.
 */

import { Component } from 'react';
import type { ErrorInfo, ReactNode } from 'react';
import { AlertTriangle, RotateCw } from 'lucide-react';

interface Props {
  children: ReactNode;
  /** Changing this clears the current error — pass the active screen id. */
  resetKey: string;
}

interface State {
  error: Error | null;
  /** The key the current error was captured under, so a change to it can clear one. */
  resetKey: string;
}

export class ScreenErrorBoundary extends Component<Props, State> {
  state: State = { error: null, resetKey: this.props.resetKey };

  static getDerivedStateFromError(error: Error): Pick<State, 'error'> {
    return { error };
  }

  // Derived rather than a `componentDidUpdate` setState: this clears the error during
  // the same render that navigates away, instead of committing a broken screen first
  // and then re-rendering to replace it.
  static getDerivedStateFromProps(props: Props, state: State): State | null {
    if (props.resetKey === state.resetKey) return null;
    return { error: null, resetKey: props.resetKey };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // The stack is worth more than the message here: these are almost always a shape
    // mismatch between a payload and the component reading it, and the component name
    // is what points at which one.
    console.error('screen render failed', error, info.componentStack);
  }

  render(): ReactNode {
    const { error } = this.state;
    if (!error) return this.props.children;

    return (
      <div className="max-w-xl mx-auto mt-16">
        <div className="obsidian-card py-8 px-6 flex flex-col gap-3">
          <div className="flex items-start gap-2.5 text-[#ffb4ab]">
            <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
            <h2 className="text-base font-semibold">This screen could not be drawn</h2>
          </div>
          <p className="text-xs text-[#8b919d] leading-relaxed">
            The rest of the lab is unaffected — the cluster is still running and every other
            screen still works. Pick another page in the sidebar, or try this one again.
          </p>
          <pre className="terminal-block text-[11px] whitespace-pre-wrap break-words">
            {error.message}
          </pre>
          <button
            className="btn-obsidian btn-secondary btn-sm self-start"
            onClick={() => this.setState({ error: null })}
          >
            <RotateCw className="w-3 h-3" />
            Try again
          </button>
        </div>
      </div>
    );
  }
}
