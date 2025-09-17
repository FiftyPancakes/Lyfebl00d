#!/usr/bin/env python3
"""
Paper Trading Analysis Script

This script analyzes the results of paper trading to evaluate strategy performance
and compare it with backtested results. It processes the paper_trading_log.csv file
and generates comprehensive reports.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import argparse
import json
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class PaperTradingAnalyzer:
    def __init__(self, log_file="paper_trading_log.csv"):
        self.log_file = log_file
        self.df = None
        self.load_data()
    
    def load_data(self):
        """Load paper trading data from CSV file"""
        try:
            self.df = pd.read_csv(self.log_file)
            print(f"Loaded {len(self.df)} paper trading records")
        except FileNotFoundError:
            print(f"Paper trading log file {self.log_file} not found")
            self.df = pd.DataFrame()
    
    def analyze_performance(self):
        """Analyze overall performance metrics"""
        if self.df.empty:
            print("No data to analyze")
            return
        
        print("\n=== PAPER TRADING PERFORMANCE ANALYSIS ===")
        
        # Basic statistics
        total_trades = len(self.df)
        profitable_trades = len(self.df[self.df['net_profit_usd'] > 0])
        win_rate = profitable_trades / total_trades * 100 if total_trades > 0 else 0
        
        total_profit = self.df['net_profit_usd'].sum()
        total_gas_cost = self.df['gas_cost_usd'].sum()
        total_gross_profit = self.df['profit_usd'].sum()
        
        avg_profit_per_trade = self.df['net_profit_usd'].mean()
        avg_gas_cost_per_trade = self.df['gas_cost_usd'].mean()
        
        max_profit = self.df['net_profit_usd'].max()
        max_loss = self.df['net_profit_usd'].min()
        
        # Risk metrics
        profit_std = self.df['net_profit_usd'].std()
        sharpe_ratio = avg_profit_per_trade / profit_std if profit_std > 0 else 0
        
        # ROI analysis
        total_investment = self.df['initial_amount_wei'].sum() / 1e18  # Convert to ETH
        overall_roi = (total_profit / total_gas_cost * 100) if total_gas_cost > 0 else 0
        
        print(f"Total Trades: {total_trades}")
        print(f"Profitable Trades: {profitable_trades}")
        print(f"Win Rate: {win_rate:.2f}%")
        print(f"Total Net Profit: ${total_profit:.6f}")
        print(f"Total Gas Cost: ${total_gas_cost:.6f}")
        print(f"Total Gross Profit: ${total_gross_profit:.6f}")
        print(f"Average Profit per Trade: ${avg_profit_per_trade:.6f}")
        print(f"Average Gas Cost per Trade: ${avg_gas_cost_per_trade:.6f}")
        print(f"Max Profit: ${max_profit:.6f}")
        print(f"Max Loss: ${max_loss:.6f}")
        print(f"Profit Standard Deviation: ${profit_std:.6f}")
        print(f"Sharpe Ratio: {sharpe_ratio:.4f}")
        print(f"Overall ROI: {overall_roi:.2f}%")
        
        return {
            'total_trades': total_trades,
            'profitable_trades': profitable_trades,
            'win_rate': win_rate,
            'total_profit': total_profit,
            'total_gas_cost': total_gas_cost,
            'avg_profit_per_trade': avg_profit_per_trade,
            'sharpe_ratio': sharpe_ratio,
            'overall_roi': overall_roi
        }
    
    def analyze_temporal_patterns(self):
        """Analyze trading patterns over time"""
        if self.df.empty:
            return
        
        print("\n=== TEMPORAL ANALYSIS ===")
        
        # Convert timestamp to datetime
        self.df['datetime'] = pd.to_datetime(self.df['timestamp'], unit='s')
        self.df['date'] = self.df['datetime'].dt.date
        self.df['hour'] = self.df['datetime'].dt.hour
        
        # Daily analysis
        daily_stats = self.df.groupby('date').agg({
            'net_profit_usd': ['count', 'sum', 'mean'],
            'gas_cost_usd': 'sum',
            'roi_percentage': 'mean'
        }).round(6)
        
        print("\nDaily Trading Statistics:")
        print(daily_stats)
        
        # Hourly analysis
        hourly_stats = self.df.groupby('hour').agg({
            'net_profit_usd': ['count', 'sum', 'mean'],
            'roi_percentage': 'mean'
        }).round(6)
        
        print("\nHourly Trading Statistics:")
        print(hourly_stats)
        
        return daily_stats, hourly_stats
    
    def analyze_route_complexity(self):
        """Analyze performance by route complexity"""
        if self.df.empty:
            return
        
        print("\n=== ROUTE COMPLEXITY ANALYSIS ===")
        
        route_stats = self.df.groupby('route_legs').agg({
            'net_profit_usd': ['count', 'sum', 'mean'],
            'gas_cost_usd': 'mean',
            'roi_percentage': 'mean'
        }).round(6)
        
        print("Performance by Route Complexity:")
        print(route_stats)
        
        return route_stats
    
    def compare_with_backtest(self, backtest_file):
        """Compare paper trading results with backtest results"""
        try:
            with open(backtest_file, 'r') as f:
                backtest_data = json.load(f)
            
            print("\n=== COMPARISON WITH BACKTEST ===")
            
            # Extract backtest metrics
            backtest_profit = backtest_data.get('total_profit_usd', 0)
            backtest_trades = backtest_data.get('total_trades', 0)
            backtest_win_rate = backtest_data.get('win_rate', 0)
            
            # Paper trading metrics
            paper_profit = self.df['net_profit_usd'].sum() if not self.df.empty else 0
            paper_trades = len(self.df) if not self.df.empty else 0
            paper_win_rate = (len(self.df[self.df['net_profit_usd'] > 0]) / len(self.df) * 100) if not self.df.empty else 0
            
            print(f"Backtest Total Profit: ${backtest_profit:.6f}")
            print(f"Paper Trading Total Profit: ${paper_profit:.6f}")
            print(f"Profit Difference: ${paper_profit - backtest_profit:.6f}")
            print(f"Profit Ratio (Paper/Backtest): {paper_profit / backtest_profit:.4f}" if backtest_profit > 0 else "N/A")
            
            print(f"\nBacktest Total Trades: {backtest_trades}")
            print(f"Paper Trading Total Trades: {paper_trades}")
            print(f"Trade Count Difference: {paper_trades - backtest_trades}")
            
            print(f"\nBacktest Win Rate: {backtest_win_rate:.2f}%")
            print(f"Paper Trading Win Rate: {paper_win_rate:.2f}%")
            print(f"Win Rate Difference: {paper_win_rate - backtest_win_rate:.2f}%")
            
        except FileNotFoundError:
            print(f"Backtest file {backtest_file} not found")
        except Exception as e:
            print(f"Error comparing with backtest: {e}")
    
    def generate_plots(self, output_dir="paper_trading_analysis"):
        """Generate visualization plots"""
        if self.df.empty:
            print("No data to plot")
            return
        
        Path(output_dir).mkdir(exist_ok=True)
        
        # Set style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        # 1. Profit over time
        plt.figure(figsize=(12, 6))
        self.df['cumulative_profit'] = self.df['net_profit_usd'].cumsum()
        plt.plot(self.df['datetime'], self.df['cumulative_profit'])
        plt.title('Cumulative Profit Over Time')
        plt.xlabel('Time')
        plt.ylabel('Cumulative Profit (USD)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/cumulative_profit.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        # 2. Profit distribution
        plt.figure(figsize=(10, 6))
        plt.hist(self.df['net_profit_usd'], bins=50, alpha=0.7, edgecolor='black')
        plt.title('Profit Distribution')
        plt.xlabel('Profit (USD)')
        plt.ylabel('Frequency')
        plt.axvline(x=0, color='red', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/profit_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        # 3. ROI vs Gas Cost
        plt.figure(figsize=(10, 6))
        plt.scatter(self.df['gas_cost_usd'], self.df['roi_percentage'], alpha=0.6)
        plt.title('ROI vs Gas Cost')
        plt.xlabel('Gas Cost (USD)')
        plt.ylabel('ROI (%)')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/roi_vs_gas_cost.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        # 4. Route complexity vs performance
        plt.figure(figsize=(10, 6))
        route_performance = self.df.groupby('route_legs')['net_profit_usd'].mean()
        plt.bar(route_performance.index, route_performance.values)
        plt.title('Average Profit by Route Complexity')
        plt.xlabel('Number of Route Legs')
        plt.ylabel('Average Profit (USD)')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/route_complexity_performance.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Plots saved to {output_dir}/")
    
    def generate_report(self, output_file="paper_trading_report.txt"):
        """Generate a comprehensive text report"""
        if self.df.empty:
            print("No data to report")
            return
        
        with open(output_file, 'w') as f:
            f.write("PAPER TRADING ANALYSIS REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            # Performance metrics
            metrics = self.analyze_performance()
            f.write("PERFORMANCE METRICS\n")
            f.write("-" * 20 + "\n")
            for key, value in metrics.items():
                f.write(f"{key.replace('_', ' ').title()}: {value}\n")
            f.write("\n")
            
            # Temporal analysis
            daily_stats, hourly_stats = self.analyze_temporal_patterns()
            f.write("DAILY STATISTICS\n")
            f.write("-" * 20 + "\n")
            f.write(daily_stats.to_string())
            f.write("\n\n")
            
            f.write("HOURLY STATISTICS\n")
            f.write("-" * 20 + "\n")
            f.write(hourly_stats.to_string())
            f.write("\n\n")
            
            # Route complexity analysis
            route_stats = self.analyze_route_complexity()
            f.write("ROUTE COMPLEXITY ANALYSIS\n")
            f.write("-" * 30 + "\n")
            f.write(route_stats.to_string())
            f.write("\n\n")
            
            # Recommendations
            f.write("RECOMMENDATIONS\n")
            f.write("-" * 15 + "\n")
            
            if metrics['win_rate'] < 50:
                f.write("- Consider adjusting profit thresholds to improve win rate\n")
            if metrics['avg_profit_per_trade'] < 0:
                f.write("- Strategy may need optimization for live market conditions\n")
            if metrics['sharpe_ratio'] < 1:
                f.write("- Consider risk management improvements to reduce volatility\n")
            
            f.write("- Monitor gas costs and adjust execution timing\n")
            f.write("- Consider MEV protection for high-value trades\n")
            f.write("- Implement circuit breakers for consecutive losses\n")
        
        print(f"Report saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Analyze paper trading results')
    parser.add_argument('--log-file', default='paper_trading_log.csv', help='Paper trading log file')
    parser.add_argument('--backtest-file', help='Backtest results file for comparison')
    parser.add_argument('--output-dir', default='paper_trading_analysis', help='Output directory for plots')
    parser.add_argument('--report-file', default='paper_trading_report.txt', help='Output report file')
    
    args = parser.parse_args()
    
    analyzer = PaperTradingAnalyzer(args.log_file)
    
    # Run analysis
    analyzer.analyze_performance()
    analyzer.analyze_temporal_patterns()
    analyzer.analyze_route_complexity()
    
    if args.backtest_file:
        analyzer.compare_with_backtest(args.backtest_file)
    
    # Generate outputs
    analyzer.generate_plots(args.output_dir)
    analyzer.generate_report(args.report_file)
    
    print(f"\nAnalysis complete! Check {args.output_dir}/ for plots and {args.report_file} for detailed report.")

if __name__ == "__main__":
    main() 