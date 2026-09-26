# Accessibility statement

Vault's public dashboard follows practical WCAG 2.2 AA-oriented patterns:

- Keyboard users can use the visible skip link and every interactive link has a
  high-visibility focus indicator.
- The document declares its language, has one labelled main region, labelled
  navigation, ordered headings, and meaningful link text.
- Dynamic service status uses `role="status"` and `aria-live="polite"` so it is
  announced without interrupting a screen reader.
- Decorative topology arrows and status dots are hidden from assistive
  technology; the topology's text remains available.
- The responsive layout works at narrow viewport widths, supports forced-colors
  mode, and respects the operating system's reduced-motion preference.
- Endpoint state is available as plain JSON (`/healthz`, `/architecture`, and
  `/limitations`) as a non-visual alternative to the dashboard.

## Regression test

Run `python -m pytest tests/unit/test_cloud_demo_accessibility.py -q` to verify
the mandatory semantic and keyboard-accessibility hooks.
