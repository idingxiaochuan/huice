from backtest_gui.fund_data_fetcher import FundDataFetcher
from PyQt5.QtWidgets import QApplication
import sys

def on_progress(current, total, message):
    print(f'Progress: {current}/{total} - {message}')

def on_completed(success, message, data):
    print(f'Completed: {success} - {message}')
    if success and data is not None:
        print(f'Data shape: {data.shape}')
        print(f'First few rows:')
        print(data.head())

def on_error(message):
    print(f'Error: {message}')

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    fetcher = FundDataFetcher()
    
    # Connect signals
    fetcher.progress_signal.connect(on_progress)
    fetcher.completed_signal.connect(on_completed)
    fetcher.error_signal.connect(on_error)
    
    # Fetch data with automatic listing date detection
    symbol = '515170'
    print(f"Fetching data for {symbol} from listing date to yesterday...")
    fetcher.fetch_data(symbol, None, None, '1min', True)
    
    # Run the event loop
    sys.exit(app.exec_()) 