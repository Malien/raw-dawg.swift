import Testing

@testable import SIMDate

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

/// Behaviour agreed for non-default options, recorded ahead of implementation.
/// Remove `.disabled` once the parameters are wired through `DCCase.decode()`.
@Suite("DateComponents decoding: options", .disabled("DateComponents options are not implemented yet"))
struct DateComponentsOptionTests {

    @Test("decimalSeparator", arguments: [
        DCCase("2020-02-02T08:00:34,5", decimalSeparator: .comma, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase(reject: "2020-02-02T08:00:34.5", decimalSeparator: .comma),
        DCCase("2020-02-02T08:00:34,5", decimalSeparator: .acceptAny, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
        DCCase("2020-02-02T08:00:34.5", decimalSeparator: .acceptAny, expect: DateComponents(year: 2020, month: 2, day: 2, hour: 8, minute: 0, second: 34, nanosecond: 500_000_000)),
    ])
    func decimalSeparator(_ c: DCCase) { c.check() }
}
