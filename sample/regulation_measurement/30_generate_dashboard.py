#!/usr/bin/env python3

import argparse
import shutil
from pathlib import Path

def copy_dashboard_template(output_path):
    """
    Copy the dashboard HTML template to the specified location.
    """
    # Dashboard HTML content
    dashboard_html = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RegData Analysis Dashboard</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/PapaParse/5.3.2/papaparse.min.js"></script>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            text-align: center;
            color: #333;
            margin-bottom: 30px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        .charts-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }
        .chart-container {
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            position: relative;
            height: 400px;
        }
        .small-charts-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .small-chart-container {
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            position: relative;
            height: 300px;
        }
        .file-input-container {
            text-align: center;
            margin-top: 30px;
            padding: 20px;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .file-input-container label {
            font-weight: bold;
            margin-right: 10px;
        }
        canvas {
            max-height: 100%;
        }
        .error {
            color: red;
            text-align: center;
            margin-top: 20px;
        }
        .no-data-message {
            text-align: center;
            color: #666;
            font-style: italic;
            padding: 40px;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>RegData Analysis</h1>

        <div id="chartsContainer" style="display: none;">
            <div class="charts-grid">
                <div class="chart-container">
                    <canvas id="totalChart"></canvas>
                </div>
            </div>

            <div class="small-charts-grid">
                <div class="small-chart-container">
                    <canvas id="shallChart"></canvas>
                </div>
                <div class="small-chart-container">
                    <canvas id="mustChart"></canvas>
                </div>
                <div class="small-chart-container">
                    <canvas id="mayNotChart"></canvas>
                </div>
                <div class="small-chart-container">
                    <canvas id="requiredChart"></canvas>
                </div>
                <div class="small-chart-container">
                    <canvas id="prohibitedChart"></canvas>
                </div>
            </div>
        </div>

        <div id="noDataMessage" class="no-data-message">
            Please select a CSV file below to view the RegData analysis charts.
        </div>

        <div class="file-input-container">
            <label for="csvFile">Select regdata_analysis.csv file:</label>
            <input type="file" id="csvFile" accept=".csv">
        </div>

        <div id="error" class="error"></div>
    </div>

    <script>
        let charts = {};

        document.getElementById('csvFile').addEventListener('change', function(e) {
            const file = e.target.files[0];
            if (file) {
                Papa.parse(file, {
                    complete: function(results) {
                        processData(results.data);
                    },
                    header: true,
                    delimiter: ';',
                    skipEmptyLines: true
                });
            }
        });

        function processData(data) {
            try {
                // Clear any existing error messages
                document.getElementById('error').textContent = '';

                // Parse dates and aggregate by year
                const yearlyData = {};
                const regdataWords = ['shall', 'must', 'may not', 'required', 'prohibited'];

                data.forEach(row => {
                    if (row.date) {
                        // Parse date (format DD.MM.YY)
                        const dateParts = row.date.split('.');
                        if (dateParts.length === 3) {
                            // Convert YY to YYYY
                            const yearPart = parseInt(dateParts[2]);
                            const year = yearPart < 50 ? '20' + dateParts[2] : '19' + dateParts[2];

                            // Skip 2025 observations (incomplete year - current year)
                            if (year === '2025') {
                                return; // Skip this row
                            }

                            if (!yearlyData[year]) {
                                yearlyData[year] = {
                                    total: 0,
                                    shall: 0,
                                    must: 0,
                                    'may not': 0,
                                    required: 0,
                                    prohibited: 0
                                };
                            }

                            // Add counts for each word
                            regdataWords.forEach(word => {
                                const count = parseInt(row[word]) || 0;
                                yearlyData[year][word] += count;
                                yearlyData[year].total += count;
                            });
                        }
                    }
                });

                // Sort years chronologically
                const sortedYears = Object.keys(yearlyData).sort();

                // Create or update charts
                createCharts(sortedYears, yearlyData, regdataWords);

                // Show charts container and hide no-data message
                document.getElementById('chartsContainer').style.display = 'block';
                document.getElementById('noDataMessage').style.display = 'none';

            } catch (error) {
                document.getElementById('error').textContent = 'Error processing data: ' + error.message + '. Make sure the CSV contains a date column in DD.MM.YY format.';
                console.error('Error:', error);
            }
        }

        function createCharts(labels, yearlyData, regdataWords) {
            // Destroy existing charts if any
            Object.values(charts).forEach(chart => chart.destroy());
            charts = {};

            // Chart options template
            const chartOptions = {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                    },
                    legend: {
                        display: true,
                        position: 'top',
                    }
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: 'Year'
                        }
                    },
                    y: {
                        display: true,
                        title: {
                            display: true,
                            text: 'Count'
                        },
                        beginAtZero: true
                    }
                }
            };

            // Create total chart
            const totalCtx = document.getElementById('totalChart').getContext('2d');
            charts.total = new Chart(totalCtx, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'Total RegData Words',
                        data: labels.map(year => yearlyData[year].total),
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.2)',
                        tension: 0.1
                    }]
                },
                options: {
                    ...chartOptions,
                    plugins: {
                        ...chartOptions.plugins,
                        title: {
                            display: true,
                            text: 'Total Count of All RegData Words per Year',
                            font: {
                                size: 16
                            }
                        }
                    }
                }
            });

            // Create individual word charts
            const colors = {
                'shall': 'rgb(255, 99, 132)',
                'must': 'rgb(54, 162, 235)',
                'may not': 'rgb(255, 205, 86)',
                'required': 'rgb(75, 192, 192)',
                'prohibited': 'rgb(153, 102, 255)'
            };

            regdataWords.forEach(word => {
                const chartId = word === 'may not' ? 'mayNotChart' : word + 'Chart';
                const ctx = document.getElementById(chartId).getContext('2d');

                charts[word] = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: `"${word}" count`,
                            data: labels.map(year => yearlyData[year][word]),
                            borderColor: colors[word],
                            backgroundColor: colors[word].replace('rgb', 'rgba').replace(')', ', 0.2)'),
                            tension: 0.1
                        }]
                    },
                    options: {
                        ...chartOptions,
                        plugins: {
                            ...chartOptions.plugins,
                            title: {
                                display: true,
                                text: `Count of "${word}" per Year`,
                                font: {
                                    size: 14
                                }
                            }
                        }
                    }
                });
            });
        }
    </script>
</body>
</html>'''

    # Write the dashboard HTML
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(dashboard_html)

    print(f"Dashboard created: {output_path}")

def main():
    parser = argparse.ArgumentParser(
        description='Generate RegData analysis dashboard'
    )
    parser.add_argument(
        '--output',
        default='regdata_dashboard.html',
        help='Output HTML file path (default: regdata_dashboard.html)'
    )

    args = parser.parse_args()

    output_path = Path(args.output)
    copy_dashboard_template(output_path)

    print("\nTo use the dashboard:")
    print(f"1. Open {output_path} in your web browser")
    print("2. View the charts area (initially shows a message)")
    print("3. Scroll down and click 'Choose File' to select your regdata_analysis.csv")
    print("4. The charts will automatically appear above")
    print("\nNote: The CSV file must be semicolon-separated with a 'date' column in DD.MM.YY format")
    print("      2025 observations are automatically excluded as incomplete year data")

if __name__ == "__main__":
    main()