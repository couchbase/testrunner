import sys
import json
import subprocess
from tempfile import mkdtemp

import matplotlib
matplotlib.rcParams.update({'font.size': 9})
matplotlib.use('Agg')

from matplotlib.pyplot import figure, grid
import pylab


def mkfunc(x, pos):
    if x >= 1e9:
        return '%1.1fG' % (x * 1e-9)
    if x >= 1e6:
        return '%1.1fM' % (x * 1e-6)
    elif x >= 1e3:
        return '%1.1fK' % (x * 1e-3)
    else:
        return '%1.1f' % x


def plot_metric(metric, keys, values, outdir):
    """Plot chart and save it as PNG file"""
    mkformatter = matplotlib.ticker.FuncFormatter(mkfunc)

    fig = figure()
    ax = fig.add_subplot(1, 1, 1)

    ax.set_title(metric)
    ax.set_xlabel('Time elapsed (sec)')
    ax.yaxis.set_major_formatter(mkformatter)

    grid()

    ax.plot(keys, values, '.')
    if 'progress' in metric:
        pylab.ylim([0, 100])

    fig.savefig('{0}/{1}.png'.format(outdir, metric))


def generate_pdf(outdir):
    try:
        subprocess.call(['convert', '{0}/*'.format(outdir), 'report.pdf'])
        print("PDF report was successfully generated!")
    except OSError:
        print("All images saved to: {0}".format(outdir))


def main():
    # Read JSON dump
    filename = sys.argv[1]
    with open(filename) as fh:
        data = json.loads(fh.read())

    # Get set of metrics and chart data
    cdata = dict()
    metrics = set()
    for sample in data:
        for metric, value in sample.items():
            if metric not in cdata:
                cdata[metric] = {'keys': [], 'values': []}
            cdata[metric]['keys'].append(sample['timestamp'])
            cdata[metric]['values'].append(value)

            metrics.add(metric)

    # Generate PNG images
    outdir = mkdtemp()
    for metric in sorted(metrics):
        if metric != 'timestamp':
            cdata[metric]['keys'] = \
                [key - cdata[metric]['keys'][0] for key in cdata[metric]['keys']]
            plot_metric(metric.replace('/', '_'),
                        cdata[metric]['keys'],
                        cdata[metric]['values'],
                        outdir)

    # Generate PDF report
    generate_pdf(outdir)


if __name__ == '__main__':
    main()
