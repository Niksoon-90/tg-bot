#!/bin/bash
cd "$(dirname "$0")"
python3 sheet_diff.py
echo ""
read -p "Нажмите Enter для выхода…"
