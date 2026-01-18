
# Documentation Policy

## README.md
- The root `README.md` is the **only Markdown file** in the project.
- It serves as the landing page for **GitHub** and **PyPI** and may be included in Sphinx as the index page.

## Sphinx Documentation
- All documentation under `docs/` **must use reStructuredText (`.rst`)** format.
- Markdown files are **not allowed** in `docs/` except for the root `README.md`.
- Use Sphinx directives and roles for cross-referencing and advanced features.

## Heading Style for `.rst`
- One **double underline heading** at the top of the page (Level 1).
- Subsequent headings use **single underline** (Level 2) or `^` for Level 3 as needed.
- Example:
    ```rst
    Main Title
    ==========
    
    Section
    --------
    
    Subsection
    ^^^^^^^^^^
    ```

## Tools & Standards
- Use **Google-style docstrings** in Python code.
- Use `pathlib.Path` for file paths.
- Follow best practices for Python programming:
    - Object-Oriented Programming (OOP) where appropriate.
    - Sphinx for documentation with `.rst` files.
``
