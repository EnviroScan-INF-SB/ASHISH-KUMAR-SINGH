"""
Module 7: Final Documentation and Presentation - generator

Creates `FINAL_REPORT.md` by combining module README and docs, and exports a minimal presentation outline.

Usage:
    python scripts/final_documentation_and_presentation.py
"""

import os

ROOT = os.path.join(os.path.dirname(__file__), '..')
DOCS = os.path.join(ROOT, 'docs')
OUT = os.path.join(ROOT, 'FINAL_REPORT.md')


def build_report():
    parts = []
    parts.append('# EnviroScan - Final Project Report\n')
    if os.path.exists(DOCS):
        for fname in sorted(os.listdir(DOCS)):
            path = os.path.join(DOCS, fname)
            parts.append(f'## {fname}\n')
            with open(path,'r', encoding='utf-8') as f:
                parts.append(f.read())
                parts.append('\n---\n')
    with open(OUT,'w', encoding='utf-8') as f:
        f.write('\n'.join(parts))
    print('Final report generated at', OUT)

if __name__ == '__main__':
    build_report()
