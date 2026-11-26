## Making Changes

Depending on your changes there are certain rules you have to follow if you expect your Pull Request (PR) to be merged.

1. Check for existing pull requests that might already implement your changes.  
    You can review those pull requests with your suggestions for improvements.
1. Reference documentation or issues that are relevant to your changes.
1. If you implement a new feature, include some examples on how you intend it to be used in your PR description.
1. Write documentation for all public API methods and types.
1. Follow the libudp coding style, even if unconventional, it is kept consistent to maintain readability.
1. Try to limit your PR to one logical change or closely related changes.  
    For instance, do not refactor other code that is irrelevant to your changes. If you want to refactor something, make a dedicated PR.

### Before opening your Pull Request:

1. Run `cargo clippy --all-targets` and `cargo test` to ensure your code compiles and passes the test suite
