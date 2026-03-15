import "package:flutter/material.dart";
import "package:google_fonts/google_fonts.dart";

/*
颜色使用规范：
1. 全局背景色：surfaceContainerLowest
2. 边框都用outlineVariant颜色
3. 一般的块状都使用surfaceContainerLow 颜色（比背景深一点）
4. 需要突出的选中状态的高亮色都用primary颜色，比如按钮、tab、菜单等
*/

ThemeData defaultTheme(String theme) {
  final baseTextTheme = theme == "dark" ? ThemeData.dark().textTheme : ThemeData.light().textTheme;
  final textTheme = GoogleFonts.notoSansScTextTheme(baseTextTheme);
  final primaryTextTheme = GoogleFonts.notoSansScTextTheme(baseTextTheme);

  return ThemeData(
    useMaterial3: true,
    fontFamily: GoogleFonts.notoSansSc().fontFamily,
    colorScheme: theme == "dark" ? MaterialTheme.darkScheme() : MaterialTheme.lightScheme(),
    dividerColor: theme == "dark" ? MaterialTheme.darkScheme().outlineVariant : const Color(0xffE0E3E7),
    iconTheme: IconThemeData(
      color: theme == "dark" ? MaterialTheme.darkScheme().onSurface : MaterialTheme.lightScheme().onSurface,
    ),
    textTheme: textTheme,
    primaryTextTheme: primaryTextTheme,
  );
}

class MaterialTheme {
  final TextTheme textTheme;

  const MaterialTheme(this.textTheme);

  static ColorScheme lightScheme() {
    return const ColorScheme(
      brightness: Brightness.light,
      primary: Color(0xff3a6ea5),
      surfaceTint: Color(0xff3a6ea5),
      onPrimary: Color(0xffffffff),
      primaryContainer: Color(0xffdce6f2),
      onPrimaryContainer: Color(0xff27496b),
      secondary: Color(0xff5f6772),
      onSecondary: Color(0xffffffff),
      secondaryContainer: Color(0xffe6e8eb),
      onSecondaryContainer: Color(0xff3e444d),
      tertiary: Color(0xff6c6672),
      onTertiary: Color(0xffffffff),
      tertiaryContainer: Color(0xffece9ee),
      onTertiaryContainer: Color(0xff4e4954),
      error: Color(0xffba1a1a),
      onError: Color(0xffffffff),
      errorContainer: Color(0xffffdad6),
      onErrorContainer: Color(0xff93000a),
      surface: Color(0xfff6f6f5),
      onSurface: Color(0xff2b2b2b),
      onSurfaceVariant: Color(0xff636363),
      outline: Color(0xffCBCBCB),
      outlineVariant: Color(0xffECEDEF),
      shadow: Color(0xff000000),
      scrim: Color(0xff000000),
      inverseSurface: Color(0xff2a2a2a),
      inversePrimary: Color(0xffa6c0dd),
      primaryFixed: Color(0xffdce6f2),
      onPrimaryFixed: Color(0xff14283e),
      primaryFixedDim: Color(0xffb2c8de),
      onPrimaryFixedVariant: Color(0xff2d557c),
      secondaryFixed: Color(0xffe7eaee),
      onSecondaryFixed: Color(0xff1f252d),
      secondaryFixedDim: Color(0xffc8cfd7),
      onSecondaryFixedVariant: Color(0xff4a535f),
      tertiaryFixed: Color(0xffece8ef),
      onTertiaryFixed: Color(0xff272330),
      tertiaryFixedDim: Color(0xffd1cad8),
      onTertiaryFixedVariant: Color(0xff5a5363),
      surfaceDim: Color(0xffdfdfde),
      surfaceBright: Color(0xffffffff),
      surfaceContainerLowest: Color(0xffffffff),
      surfaceContainerLow: Color(0xffF9F9FB),
      surfaceContainer: Color(0xffF1F2F7),
      surfaceContainerHigh: Color(0xffe8e8e7),
      surfaceContainerHighest: Color(0xffe1e1e0),
    );
  }

  static ColorScheme darkScheme() {
    return const ColorScheme(
      brightness: Brightness.dark,
      primary: Color(0xff7fa6cd),
      surfaceTint: Color(0xff7fa6cd),
      onPrimary: Color(0xff13293e),
      primaryContainer: Color(0xff2a4764),
      onPrimaryContainer: Color(0xffd2e0ee),
      secondary: Color(0xffa9b0b9),
      onSecondary: Color(0xff20262e),
      secondaryContainer: Color(0xff353c45),
      onSecondaryContainer: Color(0xffcdd3db),
      tertiary: Color(0xffb0a8b7),
      onTertiary: Color(0xff2b2631),
      tertiaryContainer: Color(0xff433c4b),
      onTertiaryContainer: Color(0xffd5cedc),
      error: Color(0xffffb4ab),
      onError: Color(0xff690005),
      errorContainer: Color(0xff93000a),
      onErrorContainer: Color(0xffffdad6),
      surface: Color(0xff262626),
      onSurface: Color(0xffb2b6bc),
      onSurfaceVariant: Color(0xff8f8f8f),
      outline: Color(0xff5e5e5e), 
      outlineVariant: Color(0xff474747),
      shadow: Color(0xff000000),
      scrim: Color(0xff000000),
      inverseSurface: Color(0xffe2e2e2),
      inversePrimary: Color(0xff3f6c97),
      primaryFixed: Color(0xffd2e0ee),
      onPrimaryFixed: Color(0xff14283e),
      primaryFixedDim: Color(0xffafc5db),
      onPrimaryFixedVariant: Color(0xff2a4764),
      secondaryFixed: Color(0xffd8dee5),
      onSecondaryFixed: Color(0xff1f252d),
      secondaryFixedDim: Color(0xffbbc2cb),
      onSecondaryFixedVariant: Color(0xff4b535e),
      tertiaryFixed: Color(0xffe0d9e7),
      onTertiaryFixed: Color(0xff2a2630),
      tertiaryFixedDim: Color(0xffc4bccb),
      onTertiaryFixedVariant: Color(0xff514a5a),
      surfaceDim: Color(0xff202020),
      surfaceBright: Color(0xff3e3e3e),
      surfaceContainerLowest: Color(0xff202020),
      surfaceContainerLow: Color(0xff292929),
      surfaceContainer: Color(0xff2e2e2e),
      surfaceContainerHigh: Color(0xff363636),
      surfaceContainerHighest: Color(0xff3d3d3d),
    );
  }
}

class ExtendedColor {
  final Color seed, value;
  final ColorFamily light;
  final ColorFamily lightHighContrast;
  final ColorFamily lightMediumContrast;
  final ColorFamily dark;
  final ColorFamily darkHighContrast;
  final ColorFamily darkMediumContrast;

  const ExtendedColor({
    required this.seed,
    required this.value,
    required this.light,
    required this.lightHighContrast,
    required this.lightMediumContrast,
    required this.dark,
    required this.darkHighContrast,
    required this.darkMediumContrast,
  });
}

class ColorFamily {
  const ColorFamily({
    required this.color,
    required this.onColor,
    required this.colorContainer,
    required this.onColorContainer,
  });

  final Color color;
  final Color onColor;
  final Color colorContainer;
  final Color onColorContainer;
}
