import { Component, type ReactNode } from 'react';

interface Props {
  children: ReactNode;
  label: string;
}

interface State {
  error: Error | null;
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  render() {
    if (this.state.error) {
      return (
        <div className="error">
          {this.props.label}渲染失败：{this.state.error.message}。工作区其余部分保持可用。
        </div>
      );
    }
    return this.props.children;
  }
}
