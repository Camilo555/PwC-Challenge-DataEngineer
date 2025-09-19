#!/usr/bin/env python3
"""
Pydantic v2 Migration Script
============================

This script migrates Pydantic v1 code to v2 compatibility.
"""

import re
from pathlib import Path
from typing import List, Tuple


def migrate_file(file_path: Path) -> List[str]:
    """Migrate a single file from Pydantic v1 to v2."""
    changes = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # 1. Replace regex= with pattern=
    regex_pattern = r'Field\(([^)]*?)regex=([^,)]+)([^)]*?)\)'
    def replace_regex(match):
        before = match.group(1)
        regex_value = match.group(2)
        after = match.group(3)
        return f'Field({before}pattern={regex_value}{after})'
    
    new_content = re.sub(regex_pattern, replace_regex, content)
    if new_content != content:
        changes.append("Replaced 'regex=' with 'pattern=' in Field definitions")
        content = new_content
    
    # 2. Update root_validator imports
    if 'from pydantic import' in content and 'root_validator' in content:
        # Add model_validator to imports
        import_pattern = r'from pydantic import ([^\\n]+)'
        def update_imports(match):
            imports = match.group(1)
            if 'model_validator' not in imports:
                if imports.endswith('\\n'):
                    imports = imports[:-2]
                imports += ', model_validator'
            return f'from pydantic import {imports}'
        
        new_content = re.sub(import_pattern, update_imports, content)
        if new_content != content:
            changes.append("Added model_validator to pydantic imports")
            content = new_content
    
    # 3. Replace @root_validator with @model_validator
    root_validator_pattern = r'@root_validator\n(\s+)def (\w+)\(cls, values\):'
    def replace_root_validator(match):
        indent = match.group(1)
        func_name = match.group(2)
        return f'@model_validator(mode="before")\n{indent}@classmethod\n{indent}def {func_name}(cls, values):'
    
    new_content = re.sub(root_validator_pattern, replace_root_validator, content)
    if new_content != content:
        changes.append("Replaced @root_validator with @model_validator")
        content = new_content
    
    # 4. Update @validator to @field_validator
    validator_pattern = r'@validator\\(([\'"][^\'"]+[\'"])\\)'
    def replace_validator(match):
        field_name = match.group(1)
        return f'@field_validator({field_name})'
    
    new_content = re.sub(validator_pattern, replace_validator, content)
    if new_content != content:
        changes.append("Replaced @validator with @field_validator")
        content = new_content
    
    # Only write if there were changes
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated {file_path}")
        for change in changes:
            print(f"  - {change}")
    
    return changes


def main():
    """Main migration function."""
    print("=== Pydantic v2 Migration ===")
    print()
    
    # Find all Python files in domain entities
    domain_files = list(Path("src/domain").rglob("*.py"))
    
    all_changes = []
    for file_path in domain_files:
        if file_path.name.startswith('__'):
            continue
        
        changes = migrate_file(file_path)
        if changes:
            all_changes.extend([(str(file_path), change) for change in changes])
    
    # Quick manual fixes for specific files
    print("\nApplying specific fixes...")
    
    # Fix sales.py root_validator issues
    sales_file = Path("src/domain/entities/sales.py")
    if sales_file.exists():
        with open(sales_file, 'r') as f:
            content = f.read()
        
        # Add missing import
        if 'model_validator' not in content and 'from pydantic import' in content:
            content = content.replace(
                'from pydantic import BaseModel, Field, validator, root_validator',
                'from pydantic import BaseModel, Field, validator, root_validator, model_validator'
            )
        
        # Fix the root_validator decorators specifically
        content = content.replace(
            '@root_validator\n    def validate_financial_totals(cls, values):',
            '@model_validator(mode="before")\n    @classmethod\n    def validate_financial_totals(cls, values):'
        )
        
        content = content.replace(
            '@root_validator\n    def validate_dates(cls, values):',
            '@model_validator(mode="before")\n    @classmethod\n    def validate_dates(cls, values):'
        )
        
        with open(sales_file, 'w') as f:
            f.write(content)
        
        print(f"Applied specific fixes to {sales_file}")
    
    # Fix customers.py root_validator issues  
    customers_file = Path("src/domain/entities/customers.py")
    if customers_file.exists():
        with open(customers_file, 'r') as f:
            content = f.read()
        
        # Add missing import
        if 'model_validator' not in content and 'from pydantic import' in content:
            content = content.replace(
                'from pydantic import BaseModel, Field, validator, root_validator, EmailStr',
                'from pydantic import BaseModel, Field, validator, root_validator, EmailStr, model_validator'
            )
        
        # Fix the root_validator decorator
        content = content.replace(
            '@root_validator\n    def validate_business_fields(cls, values):',
            '@model_validator(mode="before")\n    @classmethod\n    def validate_business_fields(cls, values):'
        )
        
        with open(customers_file, 'w') as f:
            f.write(content)
        
        print(f"Applied specific fixes to {customers_file}")
    
    print(f"\n=== Migration completed ===")
    print(f"Total changes: {len(all_changes)}")
    
    if all_changes:
        print("\nSummary of changes:")
        for file_path, change in all_changes:
            print(f"  {file_path}: {change}")


if __name__ == "__main__":
    main()