import { Component, type ReactNode } from 'react';

interface Props {
  children: ReactNode;
  onError: (message: string) => void;
}

interface State {
  message: string;
}

// ErrorBoundary keeps a single broken replay or render bug from unmounting
// the whole lab: the toolbar and previous workspace stay usable.
export class ErrorBoundary extends Component<Props, State> {
  state: State = { message: '' };

  static getDerivedStateFromError(error: unknown): State {
    return { message: error instanceof Error ? error.message : String(error) };
  }

  componentDidCatch(error: unknown) {
    this.props.onError(error instanceof Error ? error.message : String(error));
  }

  render() {
    if (this.state.message) {
      return (
        <div className="error render-error" role="alert">
          <strong>当前视图渲染失败，但工作区未被破坏：</strong>
          <code>{this.state.message}</code>
          <button onClick={() => this.setState({ message: '' })}>尝试恢复视图</button>
        </div>
      );
    }
    return this.props.children;
  }
}
