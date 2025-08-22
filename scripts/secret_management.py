#!/usr/bin/env python3
"""
Secret Management CLI
Provides command-line interface for secret management operations.
"""

import asyncio
import argparse
import sys
import os
from pathlib import Path

# Fix Windows console encoding for emojis
if os.name == 'nt':  # Windows
    os.system('chcp 65001 > nul')  # Set UTF-8 encoding

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.security.secret_initialization import (
    cli_init_secrets,
    cli_rotate_secrets, 
    cli_health_check,
    get_secret_initializer
)


async def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Secret Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python secret_management.py init                    # Initialize missing secrets
  python secret_management.py init --force            # Force regenerate all secrets
  python secret_management.py rotate                  # Rotate all secrets
  python secret_management.py rotate SECRET_KEY       # Rotate specific secret
  python secret_management.py health                  # Check secret health
  python secret_management.py export --format env     # Export secrets as .env file
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize secrets')
    init_parser.add_argument(
        '--force', 
        action='store_true', 
        help='Force regeneration of existing secrets'
    )
    
    # Rotate command
    rotate_parser = subparsers.add_parser('rotate', help='Rotate secrets')
    rotate_parser.add_argument(
        'secrets', 
        nargs='*', 
        help='Specific secrets to rotate (default: all)'
    )
    
    # Health command
    subparsers.add_parser('health', help='Check secret management health')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export secrets')
    export_parser.add_argument(
        '--format',
        choices=['env', 'json', 'yaml'],
        default='env',
        help='Export format (default: env)'
    )
    export_parser.add_argument(
        '--output',
        help='Output file (default: stdout)'
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'init':
            await cli_init_secrets(force=args.force)
            
        elif args.command == 'rotate':
            secret_names = args.secrets if args.secrets else None
            await cli_rotate_secrets(secret_names)
            
        elif args.command == 'health':
            await cli_health_check()
            
        elif args.command == 'export':
            initializer = get_secret_initializer()
            export_content = await initializer.export_secrets_for_deployment(args.format)
            
            if args.output:
                output_path = Path(args.output)
                output_path.write_text(export_content)
                print(f"[OK] Secrets exported to: {output_path}")
            else:
                print(export_content)
                
    except KeyboardInterrupt:
        print("\n[CANCELLED] Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())